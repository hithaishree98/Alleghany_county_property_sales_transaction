"""Landing -> bronze ingestion, as reusable functions.

Each step is a plain function, so an orchestrator (Airflow, Dagster, Prefect) can call it
as a task and provide scheduling, retries, run history and alerting. `pipeline/run.py`
calls the same functions for local runs.

    check_not_reloading   fail if the source is refilling the file in place (row count
                          differs from what we recorded under the same last_modified)
    download_to_landing   fetch one resource unchanged into landing; fail if the source
                          changed during the download
    compare_to_active     decide what the download means vs the version in use, with evidence
    apply_comparison      promote a new version / repoint the view / remember an alias
    record_evidence       write one meta.run_log row describing what happened and why
    ingest_year           the steps above for one year

Bronze tables (bronze.sales_<year>_v<N>) are never modified after creation. A change in the
source becomes a new version; the view bronze.sales_<year> points to the version in use.

Usage:
    python -m pipeline.ingest --restore 2026:3      # rebuild a dropped version from landing
"""

import argparse
import csv
import gzip
import hashlib
import os
import re
import sys
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import pyarrow as pa

from pipeline import DATA_DIR, DB_PATH, PROJECT_ROOT
from pipeline.ckan import http_get, resource_state

LANDING_ROOT = DATA_DIR / "landing" / "real_estate_sales"
CHUNK = 1 << 20

SETUP_SQL = """
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS meta;

CREATE TABLE IF NOT EXISTS meta.load_manifest (
    load_id              VARCHAR PRIMARY KEY,
    year                 INTEGER NOT NULL,
    version              INTEGER NOT NULL,
    resource_id          VARCHAR,
    source_name          VARCHAR,
    source_url           VARCHAR,
    source_last_modified VARCHAR,
    landing_file         VARCHAR,
    file_sha256          VARCHAR,
    ckan_row_count       BIGINT,
    loaded_row_count     BIGINT,
    header_columns       VARCHAR[],
    content_hash         VARCHAR,
    raw_table            VARCHAR,
    status               VARCHAR,
    loaded_at            TIMESTAMPTZ,
    UNIQUE (year, version),
    UNIQUE (year, content_hash)
);
ALTER TABLE meta.load_manifest ADD COLUMN IF NOT EXISTS raw_table_dropped_at TIMESTAMPTZ;

CREATE TABLE IF NOT EXISTS meta.active_version (
    year       INTEGER PRIMARY KEY,
    version    INTEGER NOT NULL,
    updated_at TIMESTAMPTZ
);
-- Backfill for databases created before active_version existed.
INSERT INTO meta.active_version
SELECT year, max(version), now() FROM meta.load_manifest
WHERE year NOT IN (SELECT year FROM meta.active_version)
GROUP BY year;

-- Downloads whose bytes differ from a version's file but whose rows are identical
-- (e.g. the source re-published with renumbered _id). Lets later runs skip re-parsing.
CREATE TABLE IF NOT EXISTS meta.landing_alias (
    year           INTEGER,
    file_sha256    VARCHAR,
    version        INTEGER,
    landing_file   VARCHAR,
    ids_not_in_old BIGINT,
    first_seen     TIMESTAMPTZ,
    last_seen      TIMESTAMPTZ,
    PRIMARY KEY (year, file_sha256)
);
ALTER TABLE meta.landing_alias ADD COLUMN IF NOT EXISTS rows_with_new_id BIGINT;

CREATE TABLE IF NOT EXISTS meta.change_log (
    year            INTEGER,
    from_version    INTEGER,
    to_version      INTEGER,
    rows_before     BIGINT,
    rows_after      BIGINT,
    rows_added      BIGINT,
    rows_removed    BIGINT,
    columns_added   VARCHAR[],
    columns_removed VARCHAR[],
    diff_table      VARCHAR,
    detected_at     TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS meta.run_log (
    run_id           VARCHAR,
    year             INTEGER,
    resource_id      VARCHAR,
    landing_file     VARCHAR,
    file_sha256      VARCHAR,
    ckan_row_count   BIGINT,
    loaded_row_count BIGINT,
    status           VARCHAR,
    version          INTEGER,
    message          VARCHAR,
    started_at       TIMESTAMPTZ,
    finished_at      TIMESTAMPTZ
);
-- Evidence columns: what each run found, recorded every run.
ALTER TABLE meta.run_log ADD COLUMN IF NOT EXISTS load_id VARCHAR;
ALTER TABLE meta.run_log ADD COLUMN IF NOT EXISTS change_type VARCHAR;
ALTER TABLE meta.run_log ADD COLUMN IF NOT EXISTS ids_not_in_old BIGINT;
ALTER TABLE meta.run_log ADD COLUMN IF NOT EXISTS rows_with_new_id BIGINT;
ALTER TABLE meta.run_log ADD COLUMN IF NOT EXISTS rows_added BIGINT;
ALTER TABLE meta.run_log ADD COLUMN IF NOT EXISTS rows_removed BIGINT;
ALTER TABLE meta.run_log ADD COLUMN IF NOT EXISTS source_last_modified VARCHAR;
"""


class SourceChangedDuringDownload(RuntimeError):
    """The source reported a different state after the download than before it.
    Safe to retry later (an orchestrator's task retry does exactly that)."""


class SourceReloadInProgress(RuntimeError):
    """The source reports a different row count under a last_modified we already recorded
    with another count: it is refilling the file in place. Safe to retry later."""


@dataclass
class LandedFile:
    year: int
    path: Path
    sha256: str
    state: dict          # resource_state() at download time: last_modified, total, fields


@dataclass
class Comparison:
    change_type: str     # identical_file | republished_same_content | content_changed | reverted | new
    version: int | None = None          # version the year ends up on
    tmp_table: str | None = None        # parsed download awaiting promotion
    header: list | None = None
    content_hash: str | None = None
    loaded_row_count: int | None = None
    ids_not_in_old: int | None = None   # new _id values absent from the version we hold
    rows_with_new_id: int | None = None # rows (paired by content) whose _id differs
    rows_added: int | None = None
    rows_removed: int | None = None
    message: str | None = None


def now():
    return datetime.now(timezone.utc)


def q(name):
    return '"' + name.replace('"', '""') + '"'


def stored_path(path):
    """Path as recorded in meta tables: relative to the project root when possible."""
    path = Path(path).resolve()
    try:
        return path.relative_to(PROJECT_ROOT).as_posix()
    except ValueError:
        return path.as_posix()


def resolve_path(stored):
    path = Path(stored)
    return path if path.is_absolute() else PROJECT_ROOT / path


# ---------- setup / cleanup ----------

class WarehouseLocked(RuntimeError):
    """Another process has the warehouse file open, so it can't be opened for writing."""


def connect(db_path=DB_PATH):
    """Open the warehouse for writing. DuckDB allows a single writer per file, so a
    concurrent run (or a notebook cell that is running) fails fast here."""
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    try:
        con = duckdb.connect(str(db_path))
    except duckdb.IOException as exc:
        if "another process" not in str(exc) and "lock" not in str(exc).lower():
            raise
        pids = sorted(set(re.findall(r"PID (\d+)", str(exc))))
        raise WarehouseLocked(
            f"{db_path} is open in another process{' (PID ' + ', '.join(pids) + ')' if pids else ''}: "
            "a notebook cell that is running, a kernel started before notebooks/helpers.py closed its "
            "connection after each cell, or another pipeline run. Wait for it to finish, or shut that "
            "kernel down, and run again.") from None
    # ids_renumbered was renamed to ids_not_in_old (it counts new _id values absent from the
    # held version, not rows whose _id changed). Rename before SETUP_SQL adds the new name.
    for (table,) in con.execute(
        "SELECT table_name FROM duckdb_columns() WHERE schema_name = 'meta' AND column_name = 'ids_renumbered'"
    ).fetchall():
        con.execute(f"ALTER TABLE meta.{q(table)} RENAME COLUMN ids_renumbered TO ids_not_in_old")
    con.execute(SETUP_SQL)
    return con


def cleanup_crashed_run(con):
    """Remove leftovers of an interrupted run: temp tables and partial downloads."""
    tmp_tables = con.execute(
        "SELECT table_name FROM duckdb_tables() "
        "WHERE schema_name = 'bronze' AND table_name LIKE '\\_tmp\\_%' ESCAPE '\\'"
    ).fetchall()
    for (name,) in tmp_tables:
        con.execute(f"DROP TABLE bronze.{q(name)}")
        print(f"  cleanup: dropped leftover table bronze.{name}")
    for part in LANDING_ROOT.rglob(".tmp_*.part"):
        part.unlink()
        print(f"  cleanup: removed partial download {part}")


# ---------- landing ----------

def iter_source(url):
    with http_get(url, stream=True, timeout=300) as resp:
        yield from resp.iter_content(CHUNK)


def land(year, chunks):
    """Stream bytes unmodified into a gzip file named by the SHA-256 of the original
    (uncompressed) bytes: <year>/<sha256>.csv.gz.

    The same content always maps to the same file, so re-runs never duplicate files.
    mtime=0 and an empty filename keep the gzip output deterministic (GzipFile would
    otherwise write the random temp file name into the header).
    """
    year_dir = LANDING_ROOT / str(year)
    year_dir.mkdir(parents=True, exist_ok=True)
    part = year_dir / f".tmp_{uuid.uuid4().hex}.part"
    sha = hashlib.sha256()
    with open(part, "wb") as f, gzip.GzipFile(filename="", fileobj=f, mode="wb", compresslevel=6, mtime=0) as gz:
        for chunk in chunks:
            gz.write(chunk)
            sha.update(chunk)
    digest = sha.hexdigest()
    final = year_dir / f"{digest}.csv.gz"
    if final.exists():
        part.unlink()
    else:
        os.replace(part, final)
    return final, digest


def download_to_landing(resource):
    """Download one resource unchanged into landing, consistently: the source's state
    (last_modified, row count, columns) must be the same before and after the transfer."""
    before = resource_state(resource["id"])
    path, digest = land(resource["year"], iter_source(resource["url"]))
    after = resource_state(resource["id"])
    if before != after:
        raise SourceChangedDuringDownload(
            f"{resource['year']}: source changed during download "
            f"(last_modified {before['last_modified']} -> {after['last_modified']}, "
            f"rows {before['total']} -> {after['total']})")
    return LandedFile(resource["year"], path, digest, before)


def check_not_reloading(con, year, state):
    """The source refills a file in place and only updates last_modified once it's done, so
    mid-reload it reports a partial row count under the previous last_modified. A count that
    differs from what we recorded for the same last_modified is a reload in progress, not a
    new publication: loading it would make a half-filled file the active version."""
    lm = state["last_modified"]
    recorded = {count for (count,) in con.execute(
        "SELECT loaded_row_count FROM meta.run_log WHERE year = ? AND source_last_modified = ? "
        "AND status = 'ok' AND loaded_row_count IS NOT NULL "
        "UNION SELECT loaded_row_count FROM meta.load_manifest WHERE year = ? AND source_last_modified = ?",
        [year, lm, year, lm],
    ).fetchall()}
    if recorded and state["total"] not in recorded:
        raise SourceReloadInProgress(
            f"{year}: source reports {state['total']} rows under last_modified {lm}, "
            f"but {sorted(recorded)} were recorded for the same last_modified; reload in progress, retry later")


def open_landing(path, mode="rt"):
    path = resolve_path(path)
    kwargs = {"newline": "", "encoding": "utf-8"} if "t" in mode else {}
    if path.name.endswith(".gz"):
        return gzip.open(path, mode, **kwargs)
    return open(path, mode, **kwargs)


def file_sha256(path):
    """SHA-256 of the original source bytes (decompressing if needed)."""
    sha = hashlib.sha256()
    with open_landing(path, "rb") as f:
        while chunk := f.read(CHUNK):
            sha.update(chunk)
    return sha.hexdigest()


def read_header(path):
    with open_landing(path) as f:
        return next(csv.reader(f))


# ---------- bronze tables ----------

def build_tmp_table(con, tmp, path):
    """Parse with Python's csv module (RFC 4180) and load every value as an exact string.

    DuckDB's read_csv rejects the WPRDC dumps (header ends in CRLF, rows in LF) unless
    strict mode is turned off, which would also silently accept malformed rows. Parsing
    here keeps strictness, and empty fields stay '' instead of becoming NULL.
    """
    with open_landing(path) as f:
        reader = csv.reader(f, strict=True)
        header = next(reader)
        columns = [[] for _ in header]
        for line_no, row in enumerate(reader, start=2):
            if len(row) != len(header):
                raise ValueError(f"line {line_no}: {len(row)} fields, expected {len(header)}")
            for column, value in zip(columns, row):
                column.append(value)

    landing = pa.table({name: pa.array(values, type=pa.string()) for name, values in zip(header, columns)})
    con.register("_landing", landing)
    try:
        con.execute(f"CREATE OR REPLACE TABLE bronze.{q(tmp)} AS SELECT * FROM _landing")
    finally:
        con.unregister("_landing")


def table_columns(con, table):
    return [r[0] for r in con.execute(f"DESCRIBE bronze.{q(table)}").fetchall()]


def content_hash(con, table):
    """Order-independent fingerprint of every row, excluding CKAN's _id (a row number the
    source can reassign on re-publish). sum() rather than xor so duplicate rows don't cancel."""
    cols = [c for c in table_columns(con, table) if c != "_id"]
    row_hash = "hash(" + ", ".join(q(c) for c in cols) + ")"
    return con.execute(f"SELECT sum({row_hash}::HUGEINT)::VARCHAR FROM bronze.{q(table)}").fetchall()[0][0]


def row_count(con, table):
    return con.execute(f"SELECT count(*) FROM bronze.{q(table)}").fetchall()[0][0]


def ids_not_in_old(con, new_table, old_table):
    """How many _id values in new_table don't exist anywhere in old_table. A set comparison:
    equal to the row count proves every row got a new _id, but 0 can't tell kept _ids from
    _ids shuffled among the same numbers (see rows_with_new_id)."""
    return con.execute(
        f"SELECT count(*) FROM (SELECT _id FROM bronze.{q(new_table)} EXCEPT ALL SELECT _id FROM bronze.{q(old_table)})"
    ).fetchall()[0][0]


def rows_with_new_id(con, new_table, old_table):
    """How many rows have a different _id than the same row in old_table. Only meaningful
    when both tables hold identical rows: rows are paired on every column except _id, and
    row_number() pairs exact duplicate rows one to one."""
    cols = ", ".join(q(c) for c in table_columns(con, new_table) if c != "_id")
    numbered = "SELECT *, row_number() OVER (PARTITION BY {cols} ORDER BY _id) AS _rn FROM bronze.{table}"
    paired, changed = con.execute(
        f"SELECT count(*), count(*) FILTER (WHERE n._id <> o._id) "
        f"FROM ({numbered.format(cols=cols, table=q(new_table))}) n "
        f"JOIN ({numbered.format(cols=cols, table=q(old_table))}) o USING ({cols}, _rn)"
    ).fetchall()[0]
    if paired != row_count(con, new_table):
        raise ValueError(f"paired {paired} of {row_count(con, new_table)} rows; tables are not identical")
    return changed


def manifest_row(con, where, params):
    cur = con.execute(
        "SELECT version, file_sha256, content_hash, loaded_row_count, header_columns, raw_table, "
        "landing_file, raw_table_dropped_at "
        f"FROM meta.load_manifest WHERE {where} ORDER BY version DESC LIMIT 1",
        params,
    )
    rows = cur.fetchall()  # drain fully: DuckDB blocks the connection on an unfinished result
    if not rows:
        return None
    return dict(zip([d[0] for d in cur.description], rows[0]))


def active_row(con, year):
    """Manifest row of the version the bronze.sales_<year> view currently points to."""
    return manifest_row(
        con, "year = ? AND version = (SELECT version FROM meta.active_version WHERE year = ?)", [year, year]
    )


def known_version_for_bytes(con, year, sha):
    """Version whose data these exact bytes are known to represent (its own file or an alias)."""
    rows = con.execute(
        "SELECT version FROM meta.load_manifest WHERE year = ? AND file_sha256 = ? "
        "UNION SELECT version FROM meta.landing_alias WHERE year = ? AND file_sha256 = ?",
        [year, sha, year, sha],
    ).fetchall()
    return rows[0][0] if rows else None


def set_active(con, year, version, raw_table):
    con.execute(f"CREATE OR REPLACE VIEW bronze.{q(f'sales_{year}')} AS SELECT * FROM bronze.{q(raw_table)}")
    con.execute("INSERT OR REPLACE INTO meta.active_version VALUES (?, ?, ?)", [year, version, now()])


def verify_version(con, version_row, ckan_total):
    """The stored bronze table still matches its manifest, and the source's row count agrees."""
    table = version_row["raw_table"]
    problems = []
    if row_count(con, table) != version_row["loaded_row_count"]:
        problems.append(f"bronze.{table} row count no longer matches the manifest")
    if content_hash(con, table) != version_row["content_hash"]:
        problems.append(f"bronze.{table} content hash no longer matches the manifest")
    if ckan_total != version_row["loaded_row_count"]:
        problems.append(f"source reports {ckan_total} rows, version has {version_row['loaded_row_count']}")
    if problems:
        raise ValueError("; ".join(problems))


# ---------- compare / apply / record ----------

def compare_to_active(con, landed):
    """Decide what a download means relative to the version in use, with evidence.

    Fast path: bytes already known (a version's file or an alias) need no parsing.
    Otherwise the file is parsed into a temp table and compared by content fingerprint,
    which ignores _id and row order.
    """
    year = landed.year
    current = active_row(con, year)

    known = known_version_for_bytes(con, year, landed.sha256)
    if known is not None:
        if current and known == current["version"]:
            verify_version(con, current, landed.state["total"])
            return Comparison("identical_file", version=known, loaded_row_count=current["loaded_row_count"],
                              message="bytes already known for the active version")
        old = manifest_row(con, "year = ? AND version = ?", [year, known])
        if landed.state["total"] != old["loaded_row_count"]:
            raise ValueError(f"source reports {landed.state['total']} rows, v{known} has {old['loaded_row_count']}")
        return Comparison("reverted", version=known, loaded_row_count=old["loaded_row_count"],
                          message=f"bytes match existing v{known}")

    header = read_header(landed.path)
    if header != landed.state["fields"]:
        raise ValueError(f"file header {header} != source fields {landed.state['fields']}")

    tmp = f"_tmp_sales_{year}"
    build_tmp_table(con, tmp, landed.path)
    if table_columns(con, tmp) != header:
        raise ValueError("DuckDB columns differ from the file header")
    loaded = row_count(con, tmp)
    if loaded != landed.state["total"]:
        raise ValueError(f"row count {loaded} != source total {landed.state['total']}")
    chash = content_hash(con, tmp)

    if current and chash == current["content_hash"]:
        not_in_old = ids_not_in_old(con, tmp, current["raw_table"])
        new_id = rows_with_new_id(con, tmp, current["raw_table"])
        con.execute(f"DROP TABLE bronze.{q(tmp)}")
        return Comparison("republished_same_content", version=current["version"], content_hash=chash,
                          loaded_row_count=loaded, ids_not_in_old=not_in_old, rows_with_new_id=new_id,
                          message=f"bytes differ, rows identical; {new_id} of {loaded} rows have a new _id "
                                  f"({not_in_old} _id values not in the held version)")

    match = manifest_row(con, "year = ? AND content_hash = ?", [year, chash])
    if match:
        con.execute(f"DROP TABLE bronze.{q(tmp)}")
        return Comparison("reverted", version=match["version"], content_hash=chash, loaded_row_count=loaded,
                          message=f"rows match existing v{match['version']}")

    return Comparison("content_changed" if current else "new", tmp_table=tmp, header=header,
                      content_hash=chash, loaded_row_count=loaded)


def apply_comparison(con, resource, landed, comparison, load_id):
    """Make the comparison's outcome take effect. Every branch is idempotent or atomic."""
    year = resource["year"]
    if comparison.change_type == "identical_file":
        set_active(con, year, comparison.version, active_row(con, year)["raw_table"])
        con.execute("UPDATE meta.landing_alias SET last_seen = ? WHERE year = ? AND file_sha256 = ?",
                    [now(), year, landed.sha256])
    elif comparison.change_type == "republished_same_content":
        con.execute(
            "INSERT INTO meta.landing_alias (year, file_sha256, version, landing_file, ids_not_in_old, "
            "rows_with_new_id, first_seen, last_seen) VALUES (?, ?, ?, ?, ?, ?, ?, ?) "
            "ON CONFLICT (year, file_sha256) DO UPDATE SET last_seen = excluded.last_seen",
            [year, landed.sha256, comparison.version, stored_path(landed.path),
             comparison.ids_not_in_old, comparison.rows_with_new_id, now(), now()],
        )
    elif comparison.change_type == "reverted":
        raw_table = restore_version(con, year, comparison.version)
        set_active(con, year, comparison.version, raw_table)
        print(f"  WARNING {year}: source reverted to v{comparison.version} content")
    else:
        promote(con, resource, landed, comparison, load_id)


def promote(con, resource, landed, comparison, load_id):
    """Atomically turn the verified temp table into the next version and record it."""
    year = resource["year"]
    previous = active_row(con, year)
    con.execute("BEGIN TRANSACTION")
    try:
        version = con.execute(
            "SELECT coalesce(max(version), 0) + 1 FROM meta.load_manifest WHERE year = ?", [year]
        ).fetchall()[0][0]
        raw_table = f"sales_{year}_v{version}"
        con.execute(f"ALTER TABLE bronze.{q(comparison.tmp_table)} RENAME TO {q(raw_table)}")
        set_active(con, year, version, raw_table)
        con.execute(
            "INSERT INTO meta.load_manifest (load_id, year, version, resource_id, source_name, "
            "source_url, source_last_modified, landing_file, file_sha256, ckan_row_count, "
            "loaded_row_count, header_columns, content_hash, raw_table, status, loaded_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [load_id, year, version, resource["id"], resource["name"], resource["url"],
             landed.state["last_modified"], stored_path(landed.path), landed.sha256,
             landed.state["total"], comparison.loaded_row_count, comparison.header,
             comparison.content_hash, raw_table, comparison.change_type, now()],
        )
        if previous:
            comparison.rows_added, comparison.rows_removed = record_change(con, year, previous, version, raw_table)
        con.execute("COMMIT")
    except Exception:
        con.execute("ROLLBACK")
        raise
    comparison.version, comparison.tmp_table = version, None


def record_change(con, year, previous, version, raw_table):
    """Save the exact added/removed rows (all columns except _id) and a summary row."""
    old_table = previous["raw_table"]
    old_cols = table_columns(con, old_table)
    new_cols = table_columns(con, raw_table)
    common = [c for c in new_cols if c in old_cols and c != "_id"]
    sel = ", ".join(q(c) for c in common)
    diff_table = f"diff_{year}_v{previous['version']}_v{version}"

    con.execute(
        f"CREATE TABLE meta.{q(diff_table)} AS "
        f"SELECT 'added' AS _change, * FROM (SELECT {sel} FROM bronze.{q(raw_table)} "
        f"EXCEPT ALL SELECT {sel} FROM bronze.{q(old_table)}) "
        f"UNION ALL "
        f"SELECT 'removed' AS _change, * FROM (SELECT {sel} FROM bronze.{q(old_table)} "
        f"EXCEPT ALL SELECT {sel} FROM bronze.{q(raw_table)})"
    )
    added, removed = con.execute(
        f"SELECT count(*) FILTER (WHERE _change = 'added'), count(*) FILTER (WHERE _change = 'removed') "
        f"FROM meta.{q(diff_table)}"
    ).fetchall()[0]
    con.execute(
        "INSERT INTO meta.change_log VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [year, previous["version"], version, row_count(con, old_table), row_count(con, raw_table),
         added, removed, [c for c in new_cols if c not in old_cols],
         [c for c in old_cols if c not in new_cols], f"meta.{diff_table}", now()],
    )
    print(f"  WARNING {year}: source changed v{previous['version']} -> v{version} "
          f"(+{added} / -{removed} rows, see meta.{diff_table})")
    return added, removed


def record_evidence(con, run_id, load_id, resource, landed, comparison, started_at, error=None):
    """One meta.run_log row per year per run: what was downloaded and what it meant."""
    con.execute(
        "INSERT INTO meta.run_log (run_id, load_id, year, resource_id, landing_file, file_sha256, "
        "ckan_row_count, loaded_row_count, status, version, message, started_at, finished_at, "
        "change_type, ids_not_in_old, rows_with_new_id, rows_added, rows_removed, source_last_modified) "
        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        [run_id, load_id, resource["year"], resource["id"],
         stored_path(landed.path) if landed else None, landed.sha256 if landed else None,
         landed.state["total"] if landed else None,
         comparison.loaded_row_count if comparison else None,
         "failed" if error else "ok",
         comparison.version if comparison else None,
         str(error)[:1000] if error else (comparison.message if comparison else None),
         started_at, now(),
         "failed" if error else comparison.change_type,
         comparison.ids_not_in_old if comparison else None,
         comparison.rows_with_new_id if comparison else None,
         comparison.rows_added if comparison else None,
         comparison.rows_removed if comparison else None,
         landed.state["last_modified"] if landed else None],
    )


# ---------- retention / restore ----------

def apply_retention(con, year, keep):
    """Drop full snapshots beyond the newest `keep` versions of a year.

    The active version is always kept, and a table is dropped only when its landing
    file exists and still hashes to the manifest's file_sha256, so it can be restored.
    """
    active = con.execute("SELECT version FROM meta.active_version WHERE year = ?", [year]).fetchall()
    active = active[0][0] if active else None
    versions = con.execute(
        "SELECT version, raw_table, landing_file, file_sha256 FROM meta.load_manifest "
        "WHERE year = ? AND raw_table_dropped_at IS NULL ORDER BY version DESC",
        [year],
    ).fetchall()
    for version, raw_table, landing_file, sha in versions[keep:]:
        if version == active:
            continue
        if not landing_file or not resolve_path(landing_file).exists() or file_sha256(landing_file) != sha:
            print(f"  retention: keeping bronze.{raw_table}; its landing file is missing or does not match")
            continue
        con.execute("BEGIN TRANSACTION")
        try:
            con.execute(f"DROP TABLE bronze.{q(raw_table)}")
            con.execute("UPDATE meta.load_manifest SET raw_table_dropped_at = ? WHERE year = ? AND version = ?",
                        [now(), year, version])
            con.execute("COMMIT")
        except Exception:
            con.execute("ROLLBACK")
            raise
        print(f"  retention: dropped bronze.{raw_table} (restorable from {landing_file})")


def restore_version(con, year, version):
    """Rebuild a dropped bronze.sales_<year>_v<version> from its landing file, verified
    against the manifest. Does not change which version is active."""
    m = manifest_row(con, "year = ? AND version = ?", [year, version])
    if m is None:
        raise ValueError(f"no version {version} recorded for {year}")
    if m["raw_table_dropped_at"] is None:
        return m["raw_table"]
    path = resolve_path(m["landing_file"] or "")
    if not path.is_file():
        raise FileNotFoundError(f"landing file for {year} v{version} is missing: {path}")
    if file_sha256(path) != m["file_sha256"]:
        raise ValueError(f"landing file {path} no longer matches the manifest sha256")

    tmp = f"_tmp_restore_{year}"
    try:
        build_tmp_table(con, tmp, path)
        if row_count(con, tmp) != m["loaded_row_count"] or content_hash(con, tmp) != m["content_hash"]:
            raise ValueError(f"rebuilt {year} v{version} does not match the manifest row count/content hash")
        con.execute("BEGIN TRANSACTION")
        try:
            con.execute(f"ALTER TABLE bronze.{q(tmp)} RENAME TO {q(m['raw_table'])}")
            con.execute("UPDATE meta.load_manifest SET raw_table_dropped_at = NULL WHERE year = ? AND version = ?",
                        [year, version])
            con.execute("COMMIT")
        except Exception:
            con.execute("ROLLBACK")
            raise
    finally:
        con.execute(f"DROP TABLE IF EXISTS bronze.{q(tmp)}")
    print(f"  restore: rebuilt bronze.{m['raw_table']} from {path} (row count and content hash verified)")
    return m["raw_table"]


# ---------- one year ----------

def ingest_year(con, resource, run_id, keep_versions=2):
    """Download, compare, apply and record one year. Raises on failure after recording it,
    so an orchestrator marks the task failed and can retry it."""
    load_id = uuid.uuid4().hex
    started_at = now()
    landed = comparison = None
    try:
        check_not_reloading(con, resource["year"], resource_state(resource["id"]))  # before downloading
        landed = download_to_landing(resource)
        check_not_reloading(con, landed.year, landed.state)  # the source may have moved on meanwhile
        comparison = compare_to_active(con, landed)
        apply_comparison(con, resource, landed, comparison, load_id)
    except Exception as exc:
        tmp = f"_tmp_sales_{resource['year']}"
        con.execute(f"DROP TABLE IF EXISTS bronze.{q(tmp)}")
        record_evidence(con, run_id, load_id, resource, landed, comparison, started_at, error=exc)
        raise
    record_evidence(con, run_id, load_id, resource, landed, comparison, started_at)
    apply_retention(con, resource["year"], keep_versions)
    return comparison


# ---------- CLI (restore only; runs go through pipeline.run) ----------

def parse_restore(value):
    year, _, version = value.partition(":")
    return int(year), int(version)


def main(argv=None):
    parser = argparse.ArgumentParser(description="Rebuild a dropped bronze version from its landing file.")
    parser.add_argument("--restore", type=parse_restore, metavar="YEAR:VERSION", required=True)
    parser.add_argument("--db", default=str(DB_PATH))
    args = parser.parse_args(argv)
    con = connect(args.db)
    try:
        restore_version(con, *args.restore)
    finally:
        con.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
