"""A small synthetic warehouse with the same bronze layout as the real one.

Every row starts out clean: a valid sale (code 0) of one parcel on its own deed. A test then
changes a few rows with SQL to create the problem it wants to check.
"""

import sys
import uuid
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from pipeline.ingest import connect, q, set_active  # noqa: E402

ROWS = 2000
YEAR = 2020


def bronze_rows_sql(n, year):
    """n clean rows; row i has parcel ...i, deed (10000+i)/1 and price 100000+i."""
    return f"""
        SELECT (1000000 + i)::VARCHAR AS _id,
               printf('0001A%011d', i) AS PARID,
               '1 MAIN ST, PITTSBURGH, PA 15219' AS FULL_ADDRESS,
               '1' AS PROPERTYHOUSENUM, ' ' AS PROPERTYFRACTION, '' AS PROPERTYADDRESSDIR,
               'MAIN' AS PROPERTYADDRESSSTREET, 'ST' AS PROPERTYADDRESSSUF, '' AS PROPERTYADDRESSUNITDESC,
               '' AS PROPERTYUNITNO, 'PITTSBURGH' AS PROPERTYCITY, 'PA' AS PROPERTYSTATE, '15219' AS PROPERTYZIP,
               '47' AS SCHOOLCODE, 'Pittsburgh' AS SCHOOLDESC, '101' AS MUNICODE, '1st Ward - PITTSBURGH' AS MUNIDESC,
               '{year}-06-20' AS RECORDDATE, '{year}-06-15' AS SALEDATE, (100000 + i)::VARCHAR AS PRICE,
               (10000 + i)::VARCHAR AS DEEDBOOK, '1' AS DEEDPAGE, '0' AS SALECODE, 'VALID SALE' AS SALEDESC,
               'DE' AS INSTRTYP, 'DEED' AS INSTRTYPDESC
        FROM range({n}) t(i)
    """


def add_version(con, year, version, select_sql, edits=()):
    """Create bronze.sales_<year>_v<version> from select_sql, apply edits, make it active."""
    raw = f"sales_{year}_v{version}"
    con.execute(f"CREATE TABLE bronze.{q(raw)} AS {select_sql}")
    for edit in edits:
        con.execute(edit.format(t=f"bronze.{q(raw)}"))
    rows = con.execute(f"SELECT count(*) FROM bronze.{q(raw)}").fetchall()[0][0]
    con.execute(
        "INSERT INTO meta.load_manifest (load_id, year, version, source_last_modified, loaded_row_count, "
        "raw_table, status) VALUES (?, ?, ?, ?, ?, ?, 'test')",
        [uuid.uuid4().hex, year, version, f"{year}-12-31T07:00:00", rows, raw])
    set_active(con, year, version, raw)


@pytest.fixture
def warehouse(tmp_path):
    """An open connection to a fresh warehouse with one clean year loaded as v1."""
    con = connect(tmp_path / "warehouse.duckdb")
    add_version(con, YEAR, 1, bronze_rows_sql(ROWS, YEAR))
    yield con
    con.close()
