"""Silver and gold on a synthetic warehouse: the checks fail on bad data, bad rows are
quarantined, a failed build leaves the live tables alone, and running twice changes nothing.

Run from the project root:  python -m pytest -q
"""

import pytest

from conftest import ROWS, YEAR, add_version, bronze_rows_sql
from pipeline.checks import GOLD_CHECKS, CheckFailed, run_checks, table_exists
from pipeline.gold import GOLD, build_gold
from pipeline.run import build_silver_and_gold
from pipeline.silver import PREVIOUS, build_silver

TABLES = ["silver.property_sale_records", "silver.deed_reference", "silver.rejected_records", GOLD]


def one(con, sql):
    return con.execute(sql).fetchall()[0][0]


def fingerprint(con):
    return {t: con.execute(f"SELECT count(*), sum(hash(x))::HUGEINT FROM {t} AS x").fetchall()[0] for t in TABLES}


def check_failing(con, name):
    """How many rows the named check counted in the latest build."""
    return one(con, f"SELECT failing FROM meta.check_results WHERE check_name = '{name}' "
                    "ORDER BY checked_at DESC LIMIT 1")


def build(con):
    build_silver(con)
    build_gold(con)


def new_version(con, edits):
    """The source publishes a changed file: a v2 of the test year with the edits applied."""
    add_version(con, YEAR, 2, bronze_rows_sql(ROWS, YEAR), edits)


# ---------- re-runnable ----------

def test_clean_build_and_rerun_change_nothing(warehouse):
    build(warehouse)
    first = fingerprint(warehouse)
    build(warehouse)
    assert fingerprint(warehouse) == first
    assert one(warehouse, "SELECT count(*) FROM silver.property_sale_records") == ROWS
    assert one(warehouse, f"SELECT sum(valid_sales_used) FROM {GOLD}") == ROWS


# ---------- rows that can't be typed: quarantined, or the build stops ----------

def test_a_few_unparseable_rows_are_quarantined(warehouse):
    new_version(warehouse, ["UPDATE {t} SET SALEDATE = '15/06/2020' WHERE _id = '1000005'"])
    build(warehouse)
    assert one(warehouse, "SELECT count(*) FROM silver.property_sale_records") == ROWS - 1
    assert one(warehouse, "SELECT reject_reason FROM silver.rejected_records") == "SALEDATE is not a yyyy-mm-dd date"
    assert check_failing(warehouse, "rejected_rows") == 1          # a warning, the build went on


def test_sale_date_after_the_file_date_is_quarantined(warehouse):
    """The test file was last modified on 2020-12-31, so a 2021 sale date can't be real."""
    new_version(warehouse, ["UPDATE {t} SET SALEDATE = '2021-03-01' WHERE _id = '1000005'"])
    build(warehouse)
    assert one(warehouse, "SELECT count(*) FROM silver.property_sale_records") == ROWS - 1
    assert one(warehouse, "SELECT reject_reason FROM silver.rejected_records").startswith("SALEDATE is before 2012-01-01")
    assert check_failing(warehouse, "sale_date_in_range") == 0          # nothing out of range reached silver


def test_switch_to_documented_date_format_stops_the_build(warehouse):
    """The data dictionary says dd/mm/yyyy but the files use yyyy-mm-dd (02 §3.2). If a file
    ever switched, most dates would fail to parse: the build stops, silver stays as it was."""
    build(warehouse)
    before = fingerprint(warehouse)
    new_version(warehouse, ["UPDATE {t} SET SALEDATE = strftime(SALEDATE::DATE, '%d/%m/%Y')"])
    with pytest.raises(CheckFailed, match="reject_rate_per_year"):
        build_silver(warehouse)
    assert fingerprint(warehouse) == before
    assert one(warehouse, "SELECT status FROM meta.build_log ORDER BY started_at DESC LIMIT 1") == "failed"


def test_renamed_column_stops_the_build(warehouse):
    build(warehouse)
    before = fingerprint(warehouse)
    add_version(warehouse, YEAR, 2, bronze_rows_sql(ROWS, YEAR).replace("AS SALEDATE", "AS SALE_DATE"))
    with pytest.raises(CheckFailed, match="bronze columns changed"):
        build_silver(warehouse)
    assert fingerprint(warehouse) == before


def test_bronze_not_matching_its_manifest_stops_the_build(warehouse):
    warehouse.execute("UPDATE meta.load_manifest SET loaded_row_count = loaded_row_count + 1")
    with pytest.raises(CheckFailed, match="bronze_matches_manifest"):
        build_silver(warehouse)


# ---------- findings from this dataset ----------

def test_placeholder_deeds_are_flagged_and_never_grouped(warehouse):
    """0/0 is not a document; in the real data it is shared by 957 unrelated parcels (01 §2.4)."""
    new_version(warehouse, ["UPDATE {t} SET DEEDBOOK = '0', DEEDPAGE = '00' WHERE _id IN ('1000001', '1000002', '1000003')"])
    build(warehouse)
    assert check_failing(warehouse, "placeholder_deed_rows") == 3
    assert one(warehouse, "SELECT count(*) FROM silver.deed_reference WHERE deed_book IN ('0', '00')") == 0
    assert one(warehouse, f"SELECT sum(valid_sales_used) FROM {GOLD}") == ROWS - 3
    assert one(warehouse, f"SELECT sum(valid_records_excluded) FROM {GOLD}") == 3


def test_multi_parcel_deed_price_is_not_used_per_parcel(warehouse):
    """A multi-parcel deed repeats the deed's total on every parcel (03 §4.3)."""
    new_version(warehouse, ["UPDATE {t} SET DEEDBOOK = '99999', PRICE = '900000' WHERE _id IN ('1000001', '1000002', '1000003')"])
    build(warehouse)
    assert one(warehouse, "SELECT parcel_count FROM silver.deed_reference WHERE deed_book = '99999'") == 3
    assert one(warehouse, f"SELECT sum(valid_sales_used) FROM {GOLD}") == ROWS - 3
    assert one(warehouse, f"SELECT sum(valid_records_excluded) FROM {GOLD}") == 3


def test_two_valid_rows_for_one_sale_stop_gold(warehouse):
    """Today no repeated parcel + deed + date group has two valid rows (03 §4.7 B). If one
    appears, gold can't tell which price is right, so gold stops instead of picking one."""
    build(warehouse)
    gold_before = one(warehouse, f"SELECT sum(hash(x))::HUGEINT FROM {GOLD} AS x")
    new_version(warehouse, ["""UPDATE {t} SET PARID = '0001A00000000001', DEEDBOOK = '10001', SALECODE = 'U',
                                              SALEDESC = 'OTHER', PRICE = '250000' WHERE _id = '1000002'"""])
    build_silver(warehouse)                                     # silver keeps both rows and flags them
    assert check_failing(warehouse, "repeated_parcel_deed_date_rows") == 2
    with pytest.raises(CheckFailed, match="gold_one_valid_row_per_sale"):
        build_gold(warehouse)
    assert one(warehouse, f"SELECT sum(hash(x))::HUGEINT FROM {GOLD} AS x") == gold_before


TWO_VALID_ROWS_FOR_ONE_SALE = """UPDATE {t} SET PARID = '0001A00000000001', DEEDBOOK = '10001', SALECODE = 'U',
                                                SALEDESC = 'OTHER', PRICE = '250000' WHERE _id = '1000002'"""


def test_gold_failure_switches_silver_back(warehouse):
    """Silver passes its checks but gold fails: silver goes back to the previous build, so the
    live silver and gold still come from the same build."""
    build_silver_and_gold(warehouse, "first")
    before = fingerprint(warehouse)
    new_version(warehouse, [TWO_VALID_ROWS_FOR_ONE_SALE])
    with pytest.raises(CheckFailed, match="gold_one_valid_row_per_sale"):
        build_silver_and_gold(warehouse, "second")
    assert fingerprint(warehouse) == before
    assert not any(table_exists(warehouse, t) for t in PREVIOUS.values())
    assert one(warehouse, "SELECT status FROM meta.build_log WHERE layer = 'silver' "
                          "ORDER BY finished_at DESC LIMIT 1") == "rolled_back"


def test_gold_failure_on_the_first_build_leaves_no_silver(warehouse):
    new_version(warehouse, [TWO_VALID_ROWS_FOR_ONE_SALE])
    with pytest.raises(CheckFailed, match="gold_one_valid_row_per_sale"):
        build_silver_and_gold(warehouse, "first")
    assert not any(table_exists(warehouse, t) for t in TABLES + list(PREVIOUS.values()))


def test_same_parcel_and_date_on_two_deeds_is_not_counted_twice(warehouse):
    """60 parcels have valid rows on one sale date under two different single-parcel deeds
    (03 §4.4). One sale recorded twice or two transfers: neither price is used."""
    new_version(warehouse, ["UPDATE {t} SET PARID = '0001A00000000001' WHERE _id = '1000002'"])
    build(warehouse)
    assert one(warehouse, f"SELECT sum(valid_sales_used) FROM {GOLD}") == ROWS - 2
    assert one(warehouse, f"SELECT sum(valid_records_excluded) FROM {GOLD}") == 2
    assert check_failing(warehouse, "gold_valid_rows_reconcile") == 0
    assert check_failing(warehouse, "gold_one_sale_per_parcel_date") == 0


def test_one_sale_per_parcel_date_check_fails_on_a_repeated_sale(warehouse):
    """The guard behind the rule above: if a parcel and sale date reach gold twice, gold stops."""
    warehouse.execute("""CREATE TABLE repeated_observations AS
                         SELECT * FROM (VALUES ('0001A00000000001', DATE '2020-06-15'),
                                               ('0001A00000000001', DATE '2020-06-15')) t(parcel_id, sale_date)""")
    check = [c for c in GOLD_CHECKS if c.name == "gold_one_sale_per_parcel_date"]
    with pytest.raises(CheckFailed, match="gold_one_sale_per_parcel_date"):
        run_checks(warehouse, check, {"observations": "repeated_observations"}, "gold", "test")


def test_valid_sale_below_price_floor_warns_and_is_still_used(warehouse):
    """Valid-coded sales under $1,000 are reported, not dropped: the sale code decides."""
    new_version(warehouse, ["UPDATE {t} SET PRICE = '500' WHERE _id = '1000004'"])
    build(warehouse)
    assert check_failing(warehouse, "valid_sale_price_floor") == 1
    assert one(warehouse, f"SELECT sum(valid_sales_used) FROM {GOLD}") == ROWS


def test_unknown_sale_code_warns_and_never_counts_as_valid(warehouse):
    new_version(warehouse, ["UPDATE {t} SET SALECODE = 'QQ' WHERE _id = '1000007'"])
    build(warehouse)
    assert check_failing(warehouse, "sale_code_not_in_lookup") == 1
    assert one(warehouse, f"SELECT sum(valid_sales_used) FROM {GOLD}") == ROWS - 1


def test_small_cells_get_no_median(warehouse):
    new_version(warehouse, ["UPDATE {t} SET MUNICODE = '102', MUNIDESC = '2nd Ward - PITTSBURGH' WHERE _id IN ('1000001', '1000002')"])
    build(warehouse)
    small = warehouse.execute(f"SELECT valid_sales_used, median_price, is_low_sample FROM {GOLD} WHERE muni_code = '102'").fetchall()
    assert small == [(2, None, True)]
