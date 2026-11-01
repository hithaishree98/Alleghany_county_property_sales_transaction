"""Bronze -> silver: one typed, cleaned row per source record.

    source_view       every active bronze year stacked, with its version (lineage)
    check_structure   every active year has the 26 expected columns, in order; stop otherwise
    stage_tables      typed and trimmed rows with flags for known source limitations, the rows
                      that cannot be typed (rejected), and one row per real deed reference
    build_silver      the steps above, the silver checks, then one atomic swap

Silver keeps the source grain: nothing is deduplicated, filtered or corrected. Known
problems in the source (placeholder deed references, repeated parcel + deed + date groups,
odd record dates, unlisted sale codes) stay as rows with a flag. Only a row that cannot be
typed at all (no valid parcel id, sale date, price or record date, or a sale date outside
2012 to the file's last_modified date) goes to silver.rejected_records, with the reason; it is re-checked on the next build because silver
is always rebuilt from bronze, which keeps every raw file.

Tables (replaced together, only when no hard check fails; pipeline/run.py switches them back
with restore_previous if gold then fails):
    silver.property_sale_records     one row per source record of the active bronze versions
    silver.deed_reference            one row per real deed reference (book + page), all years
    silver.rejected_records          rows that could not be typed, with the reason
    silver.ref_sale_validation_code  sale code -> validity group (pipeline/ref/*.csv)
"""

import uuid
from pathlib import Path

from pipeline.checks import EXPECTED_COLUMNS, FIRST_SALE_DATE, SILVER_CHECKS, CheckFailed, run_checks, table_exists
from pipeline.ingest import now, q

REF_CODES = Path(__file__).resolve().parent / "ref" / "sale_validation_codes.csv"
SOURCE_VIEW = "_silver_source"
STAGE = {
    "records": "silver._stage_property_sale_records",
    "deeds": "silver._stage_deed_reference",
    "rejected": "silver._stage_rejected_records",
    "codes": "silver._stage_ref_sale_validation_code",
}
LIVE = {
    "records": "silver.property_sale_records",
    "deeds": "silver.deed_reference",
    "rejected": "silver.rejected_records",
    "codes": "silver.ref_sale_validation_code",
}
# The live tables a build replaced, kept until gold is built from the new ones (see restore_previous)
PREVIOUS = {k: f"silver._prev_{v.split('.')[1]}" for k, v in LIVE.items()}

SETUP_SQL = """
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS meta;
CREATE TABLE IF NOT EXISTS meta.build_log (
    build_id         VARCHAR,
    layer            VARCHAR,
    status           VARCHAR,
    source_versions  VARCHAR,
    rows_in          BIGINT,
    rows_out         BIGINT,
    rows_rejected    BIGINT,
    message          VARCHAR,
    started_at       TIMESTAMPTZ,
    finished_at      TIMESTAMPTZ
);
"""

# A deed reference is a placeholder when its book or its page is blank or only zeros (01 §2.4).
PLACEHOLDER = "(trim(DEEDBOOK, '0 ') = '' OR trim(DEEDPAGE, '0 ') = '')"


def active_versions(con):
    return con.execute("""
        SELECT a.year, a.version, m.load_id, left(m.source_last_modified, 10)
        FROM meta.active_version a JOIN meta.load_manifest m USING (year, version)
        ORDER BY a.year
    """).fetchall()


def source_view(con, versions):
    """All active bronze years stacked, with the version each row came from."""
    con.execute(f"CREATE OR REPLACE TEMP VIEW {SOURCE_VIEW} AS " + " UNION ALL ".join(
        f"SELECT {year} AS source_year, {version} AS source_version, '{load_id}' AS source_load_id, "
        f"DATE '{file_date}' AS source_file_date, * FROM bronze.{q(f'sales_{year}')}"
        for year, version, load_id, file_date in versions))


def check_structure(con, versions):
    """Every active year must have exactly the expected columns, in order (02 §3.0). A renamed
    or missing column is a change in the source contract, so nothing is built from it."""
    problems = []
    for year, version, _, _ in versions:
        columns = [r[0] for r in con.execute(f"DESCRIBE bronze.{q(f'sales_{year}')}").fetchall()]
        if columns != EXPECTED_COLUMNS:
            missing = [c for c in EXPECTED_COLUMNS if c not in columns]
            extra = [c for c in columns if c not in EXPECTED_COLUMNS]
            problems.append(f"{year} v{version}: missing {missing}, unexpected {extra}"
                            if missing or extra else f"{year} v{version}: columns in a different order")
    if problems:
        raise CheckFailed("silver: bronze columns changed: " + "; ".join(problems))


def stage_tables(con):
    """Create the staged silver tables from the source view."""
    s = STAGE
    con.execute(f"CREATE OR REPLACE TABLE {s['codes']} AS "
                f"SELECT * FROM read_csv('{REF_CODES.as_posix()}', header = true, all_varchar = true)")

    # Type every row once; a row that can't be typed gets a reason and goes to rejected.
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE _silver_typed AS
        SELECT *,
               try_strptime(SALEDATE, '%Y-%m-%d')::DATE AS _sale_date,
               try_strptime(nullif(trim(RECORDDATE), ''), '%Y-%m-%d')::DATE AS _record_date,
               try_cast(nullif(trim(PRICE), '') AS BIGINT) AS _price,
               concat_ws('; ',
                   CASE WHEN NOT regexp_matches(PARID, '^[0-9]{{4}}[A-Z][0-9A-Z]{{11}}$')
                        THEN 'PARID is not 16 characters of digits/capitals' END,
                   CASE WHEN try_strptime(SALEDATE, '%Y-%m-%d') IS NULL
                        THEN 'SALEDATE is not a yyyy-mm-dd date' END,
                   CASE WHEN try_strptime(SALEDATE, '%Y-%m-%d')::DATE
                             NOT BETWEEN DATE '{FIRST_SALE_DATE}' AND source_file_date
                        THEN 'SALEDATE is before {FIRST_SALE_DATE} or after the file was last modified' END,
                   CASE WHEN trim(PRICE) <> '' AND (NOT regexp_matches(trim(PRICE), '^[0-9]+$')
                                                    OR try_cast(trim(PRICE) AS BIGINT) IS NULL)
                        THEN 'PRICE is not a whole number' END,
                   CASE WHEN trim(RECORDDATE) <> '' AND try_strptime(trim(RECORDDATE), '%Y-%m-%d') IS NULL
                        THEN 'RECORDDATE is not a yyyy-mm-dd date' END
               ) AS _reject_reason
        FROM {SOURCE_VIEW}
    """)

    columns = ", ".join(q(c) for c in EXPECTED_COLUMNS)
    con.execute(f"""
        CREATE OR REPLACE TABLE {s['rejected']} AS
        SELECT source_year, source_version, source_load_id, _reject_reason AS reject_reason, {columns}
        FROM _silver_typed WHERE _reject_reason <> ''
    """)

    hashed = ", ".join(q(c) for c in EXPECTED_COLUMNS if c != "_id")
    con.execute(f"""
        CREATE OR REPLACE TABLE {s['records']} AS
        WITH t AS (SELECT * FROM _silver_typed WHERE _reject_reason = '')
        SELECT
            -- lineage: which bronze row this is; _id only locates a row inside one snapshot (01 §2.2)
            source_year, source_version, source_load_id, _id AS source_row_id,
            md5(concat_ws(chr(31), {hashed})) AS record_hash,
            row_number() OVER (PARTITION BY md5(concat_ws(chr(31), {hashed})) ORDER BY source_year, _id) AS record_seq,

            PARID AS parcel_id,
            _sale_date AS sale_date,
            year(_sale_date) AS sale_year,
            _record_date AS record_date,
            _record_date - _sale_date AS record_lag_days,
            coalesce(_record_date < _sale_date, false) AS is_record_before_sale,
            coalesce(_record_date < DATE '2012-01-01' OR _record_date > source_file_date, false)
                AS is_record_date_out_of_range,
            _price AS price,

            -- deed reference: kept as written (trimmed); leading zeros are not removed (03 §4.7 D)
            nullif(trim(DEEDBOOK), '') AS deed_book_raw,
            nullif(trim(DEEDPAGE), '') AS deed_page_raw,
            CASE WHEN {PLACEHOLDER} THEN NULL ELSE trim(DEEDBOOK) END AS deed_book,
            CASE WHEN {PLACEHOLDER} THEN NULL ELSE trim(DEEDPAGE) END AS deed_page,
            {PLACEHOLDER} AS is_deed_placeholder,
            count(*) OVER (PARTITION BY PARID, DEEDBOOK, DEEDPAGE, SALEDATE) > 1 AS is_repeated_parcel_deed_date,

            -- validity comes from the code, never from the description (02 §3.4)
            SALECODE AS sale_code,
            trim(SALEDESC) AS sale_desc,
            c.validity_group AS sale_validity_group,
            INSTRTYP AS instrument_type,
            trim(INSTRTYPDESC) AS instrument_desc,

            -- parcel attributes: one value per parcel across all files, probably today's
            MUNICODE AS muni_code,
            trim(MUNIDESC) AS muni_desc,
            SCHOOLCODE AS school_code,
            trim(SCHOOLDESC) AS school_desc,
            FULL_ADDRESS AS full_address,
            nullif(trim(PROPERTYHOUSENUM), '') AS house_number,
            coalesce(nullif(trim(PROPERTYHOUSENUM), '') <> '0', false) AS has_house_number,
            nullif(trim(PROPERTYFRACTION), '') AS house_number_fraction,
            nullif(trim(PROPERTYADDRESSDIR), '') AS street_direction,
            nullif(trim(PROPERTYADDRESSSTREET), '') AS street_name,
            nullif(trim(PROPERTYADDRESSSUF), '') AS street_suffix,
            nullif(trim(PROPERTYADDRESSUNITDESC), '') AS unit_description,
            nullif(trim(PROPERTYUNITNO), '') AS unit_number,
            nullif(trim(PROPERTYCITY), '') AS city,
            nullif(trim(PROPERTYSTATE), '') AS state,
            nullif(trim(PROPERTYZIP), '') AS zip
        FROM t LEFT JOIN {s['codes']} c ON c.sale_code = t.SALECODE
    """)

    # One row per real deed reference across all years: a deed's price is the total for all
    # its parcels (03 §4.3), and the same book + page can show up in several years (03 §4.7 D).
    con.execute(f"""
        CREATE OR REPLACE TABLE {s['deeds']} AS
        SELECT deed_book, deed_page,
               count(*) AS records,
               count(DISTINCT parcel_id) AS parcel_count,
               count(DISTINCT price) AS distinct_prices,
               count(DISTINCT sale_date) AS sale_date_count,
               count(DISTINCT sale_year) AS sale_year_count,
               min(sale_date) AS first_sale_date,
               max(sale_date) AS last_sale_date
        FROM {s['records']} WHERE NOT is_deed_placeholder
        GROUP BY 1, 2
    """)
    con.execute("DROP TABLE _silver_typed")


def swap_in(con, stage, live, previous=None):
    """Replace the live tables with the staged ones in one transaction. With `previous`, the
    replaced tables are kept under those names instead of dropped, so restore_previous can
    switch back to them."""
    con.execute("BEGIN TRANSACTION")
    try:
        for key, staged in stage.items():
            schema, table = live[key].split(".")
            if previous:
                con.execute(f"DROP TABLE IF EXISTS {previous[key]}")
                if table_exists(con, live[key]):
                    con.execute(f"ALTER TABLE {schema}.{q(table)} RENAME TO {q(previous[key].split('.')[1])}")
            else:
                con.execute(f"DROP TABLE IF EXISTS {schema}.{q(table)}")
            con.execute(f"ALTER TABLE {staged} RENAME TO {q(table)}")
        con.execute("COMMIT")
    except Exception:
        con.execute("ROLLBACK")
        raise


def restore_previous(con, build_id):
    """Switch silver back to the tables the last build_silver(keep_previous=True) replaced, in
    one transaction. Used when gold fails, so silver and gold always come from the same build.
    A table with no previous version (the first build) is dropped: silver is as it was."""
    con.execute("BEGIN TRANSACTION")
    try:
        for key, prev in PREVIOUS.items():
            schema, table = LIVE[key].split(".")
            con.execute(f"DROP TABLE IF EXISTS {schema}.{q(table)}")
            if table_exists(con, prev):
                con.execute(f"ALTER TABLE {prev} RENAME TO {q(table)}")
        con.execute("COMMIT")
    except Exception:
        con.execute("ROLLBACK")
        raise
    log_build(con, build_id, "silver", "rolled_back", active_versions(con), None, None, None,
              "gold failed, so silver was switched back to the previous build", now())
    print("  silver: switched back to the previous build because gold failed")


def drop_stage(con, stage):
    for staged in stage.values():
        con.execute(f"DROP TABLE IF EXISTS {staged}")


def log_build(con, build_id, layer, status, versions, rows_in, rows_out, rows_rejected, message, started_at):
    con.execute("INSERT INTO meta.build_log VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                [build_id, layer, status, ", ".join(f"{y}:v{v}" for y, v, *_ in versions),
                 rows_in, rows_out, rows_rejected, message, started_at, now()])


def build_silver(con, build_id=None, keep_previous=False):
    """Rebuild silver from the active bronze versions. Raises CheckFailed (after logging)
    if a hard check fails; the live silver tables are then left as they were. With
    keep_previous, the replaced tables are kept for restore_previous."""
    con.execute(SETUP_SQL)
    build_id = build_id or uuid.uuid4().hex
    started_at = now()
    versions = active_versions(con)
    drop_stage(con, STAGE)  # leftovers of an interrupted build
    try:
        if not versions:
            raise CheckFailed("silver: no active bronze versions; run the ingest first")
        source_view(con, versions)
        check_structure(con, versions)
        stage_tables(con)
        run_checks(con, SILVER_CHECKS,
                   {"source": SOURCE_VIEW, "current_records": LIVE["records"],
                    **{k: v for k, v in STAGE.items()}},
                   "silver", build_id)
        rows_in = con.execute(f"SELECT count(*) FROM {SOURCE_VIEW}").fetchall()[0][0]
        rows_out = con.execute(f"SELECT count(*) FROM {STAGE['records']}").fetchall()[0][0]
        rejected = con.execute(f"SELECT count(*) FROM {STAGE['rejected']}").fetchall()[0][0]
        swap_in(con, STAGE, LIVE, PREVIOUS if keep_previous else None)
    except Exception as exc:
        drop_stage(con, STAGE)
        log_build(con, build_id, "silver", "failed", versions, None, None, None, str(exc)[:1000], started_at)
        raise
    log_build(con, build_id, "silver", "ok", versions, rows_in, rows_out, rejected, None, started_at)
    print(f"  silver: {rows_out:,} records, {rejected:,} rejected, from {len(versions)} years")
    return build_id
