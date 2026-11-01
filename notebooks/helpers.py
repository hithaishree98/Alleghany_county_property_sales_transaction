"""Shared helpers for the exploration notebooks (read-only).

    from helpers import connect, show, profile, by_year, DATA_DIR
    con, years = connect()

connect() gives a read-only connection to data/warehouse.duckdb with a temp view `s`: every
year's active bronze table stacked, with a `file_year` column saying which file each row came
from.

DuckDB allows one writer per file, and any open connection (even read-only) blocks it. So the
connection is only open while a cell runs: it opens the first time a cell uses it, and closes
when the cell finishes. The pipeline can run between cells without closing the kernel.
Anything kept on the connection itself (a registered table, a temp table) lasts one cell.
"""

import sys
from pathlib import Path

import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from pipeline import DATA_DIR, DB_PATH  # noqa: E402

_con = None


def _open():
    """The live connection, opened (with the view `s`) if this cell hasn't opened it yet."""
    global _con
    if _con is None:
        try:
            con = duckdb.connect(str(DB_PATH), read_only=True)
        except duckdb.IOException as exc:
            if "another process" not in str(exc) and "lock" not in str(exc).lower():
                raise
            raise RuntimeError("the pipeline is writing to the warehouse; run the cell again when it finishes") from None
        years = [y for (y,) in con.execute("SELECT year FROM meta.active_version ORDER BY year").fetchall()]
        con.execute("CREATE OR REPLACE TEMP VIEW s AS " + " UNION ALL ".join(
            f"SELECT {y} AS file_year, * FROM bronze.sales_{y}" for y in years))
        _con = con
    return _con


def release(*_):
    """Close the connection, so the warehouse isn't locked between cells."""
    global _con
    if _con is not None:
        _con.close()
        _con = None


class _Connection:
    """Stands in for the DuckDB connection in notebook cells: every use goes to the connection
    of the current cell, which is opened on demand."""

    def __getattr__(self, name):
        return getattr(_open(), name)


def _release_after_each_cell():
    try:
        from IPython import get_ipython
    except ImportError:
        return
    ipython = get_ipython()
    if ipython is None:  # plain Python, not a notebook
        return
    if release not in ipython.events.callbacks["post_run_cell"]:
        ipython.events.register("post_run_cell", release)


def connect():
    release()
    _release_after_each_cell()
    years = [y for (y,) in _open().execute("SELECT year FROM meta.active_version ORDER BY year").fetchall()]
    return _Connection(), years


def show(sql, rows=40, width=220):
    """Run SQL and print the result as a table."""
    _open().sql(sql).show(max_rows=rows, max_width=width)


def source_columns():
    return [c for c in _open().sql("SELECT * FROM s LIMIT 0").columns if c != "file_year"]


def profile(col, top=15):
    """One column: blanks, padding, distinct values, lengths, parse rates, leading zeros,
    and its most frequent values."""
    c = f'"{col}"'
    show(f"""
    SELECT '{col}' AS column_name,
           count(*) FILTER (WHERE {c} = '')                                  AS blank,
           count(*) FILTER (WHERE {c} <> '' AND trim({c}) = '')              AS spaces_only,
           count(*) FILTER (WHERE trim({c}) <> '' AND {c} <> trim({c}))      AS padded,
           count(*) FILTER (WHERE {c} LIKE '0%' AND length({c}) > 1)         AS leading_zero,
           count(DISTINCT {c})                                               AS distinct_values,
           min(length({c})) || '-' || max(length({c}))                       AS length_range,
           round(100 * avg((try_cast({c} AS BIGINT) IS NOT NULL)::INT) FILTER (WHERE trim({c}) <> ''), 1) AS pct_int,
           round(100 * avg((try_cast({c} AS DOUBLE) IS NOT NULL)::INT) FILTER (WHERE trim({c}) <> ''), 1) AS pct_num,
           round(100 * avg((try_strptime({c}, '%Y-%m-%d') IS NOT NULL)::INT) FILTER (WHERE trim({c}) <> ''), 1) AS pct_date
    FROM s
    """)
    show(f"SELECT {c} AS value, length({c}) AS len, count(*) AS rows FROM s GROUP BY 1 ORDER BY 3 DESC LIMIT {top}")


METRICS = {
    "blank": "trim({c}) = ''",
    "padded": "trim({c}) <> '' AND {c} <> trim({c})",
    "leading_zero": "{c} LIKE '0%' AND length({c}) > 1",
}


def by_year(metric, columns=None):
    """One metric for every column, split by file_year: a column x year matrix of row counts.
    Shows whether an issue is constant over time or concentrated in some years."""
    columns = columns or [c for c in source_columns() if c != "_id"]
    cond = METRICS[metric]
    exprs = ", ".join(
        f"count(*) FILTER (WHERE {cond.format(c=chr(34) + c + chr(34))}) AS \"{c}\"" for c in columns)
    show(f"""
    PIVOT (
        UNPIVOT (SELECT file_year, {exprs} FROM s GROUP BY file_year)
        ON COLUMNS(* EXCLUDE file_year) INTO NAME column_name VALUE n
    ) ON file_year USING sum(n) GROUP BY column_name ORDER BY column_name
    """, rows=60, width=400)


def values_by_year(col, top=20):
    """Most frequent values of one column, with their count in each file_year."""
    c = f'"{col}"'
    show(f"""
    PIVOT (
        SELECT file_year, {c} AS value FROM s
        WHERE {c} IN (SELECT {c} FROM s GROUP BY 1 ORDER BY count(*) DESC LIMIT {top})
    ) ON file_year USING count(*) GROUP BY value ORDER BY value
    """, rows=top + 5, width=400)
