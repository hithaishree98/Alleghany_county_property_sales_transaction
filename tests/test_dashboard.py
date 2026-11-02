"""The dashboard page on a synthetic warehouse: it is written from whatever gold holds, carries
every gold row, and still builds with one year (nothing to compare yet) or without the map.

Run from the project root:  python -m pytest -q
"""

import csv
import json

from conftest import ROWS, YEAR, add_version, bronze_rows_sql
from pipeline.dashboard import CSV_NAME, build_dashboard
from pipeline.gold import GOLD
from pipeline.run import build_silver_and_gold


def page_data(page):
    """The JSON the page draws from."""
    line = next(l for l in page.read_text(encoding="utf-8").splitlines() if l.startswith("const D = "))
    return json.loads(line[len("const D = "):].rstrip(";").replace("<\\/", "</"))


def gold_rows(con):
    return con.execute(f"SELECT count(*) FROM {GOLD}").fetchall()[0][0]


def test_page_carries_every_gold_row(warehouse, tmp_path):
    build_silver_and_gold(warehouse, "test")
    page = build_dashboard(warehouse, tmp_path, with_map=False)
    data = page_data(page)
    assert len(data["gold"]) == gold_rows(warehouse)
    assert data["map"] is None
    with open(tmp_path / CSV_NAME, newline="", encoding="utf-8") as f:
        assert sum(1 for _ in csv.DictReader(f)) == gold_rows(warehouse)


def test_one_year_has_nothing_to_compare_but_still_builds(warehouse, tmp_path):
    build_silver_and_gold(warehouse, "test")
    data = page_data(build_dashboard(warehouse, tmp_path, with_map=False))
    assert data["latest"] is None and data["jump"] is None    # the only year is provisional
    assert "quarters" not in data and "before_after" not in data


def test_a_final_year_is_compared(warehouse, tmp_path):
    # A second, later year with its own deeds and ids: YEAR is then final and gets compared
    add_version(warehouse, YEAR + 1, 1, bronze_rows_sql(ROWS, YEAR + 1), [
        "UPDATE {t} SET _id = (2000000 + _id::INT - 1000000)::VARCHAR, DEEDBOOK = (30000 + DEEDBOOK::INT)::VARCHAR"])
    build_silver_and_gold(warehouse, "test")
    data = page_data(build_dashboard(warehouse, tmp_path, with_map=False))
    assert data["latest"] == YEAR
    assert data["default_muni"] == "101"
    assert [r["sale_year"] for r in data["coverage"]] == [YEAR, YEAR + 1]
    assert data["ends"][str(YEAR)][0]["muni_desc"].strip() == "1st Ward - PITTSBURGH"
