"""Silver -> gold: what valid property sales typically cost, per municipality and sale year.

Question: what is the typical (median) price paid in valid, single-parcel property sales in
each municipality (or Pittsburgh/McKeesport ward) per sale year, and how many sales and how
much of the year's sale coding stand behind that number?

    observations   one per real deed reference that covers one parcel on one sale date and
                   has a valid sale code (0, U, UR) and a price; its price is counted once
    build_gold     one row per sale year x municipality, the gold checks, then a swap

Why these rules (03 §4.3, §4.4, §4.7): a multi-parcel deed repeats the deed's total price on
every parcel, so only single-parcel deeds give the price of one property; a deed reference with
several sale dates is not reliably one transfer; placeholder references are not documents; a
parcel with valid rows on one sale date under two deeds is either one sale recorded twice or two
transfers, and the data can't tell which, so neither is used. Every removed row is counted
in valid_records_excluded.

Table: gold.valid_sale_prices_by_municipality_year
"""

import uuid

from pipeline.checks import GOLD_CHECKS, CheckFailed, run_checks
from pipeline.ingest import now
from pipeline.silver import LIVE as SILVER, active_versions, drop_stage, log_build, swap_in

LOW_SAMPLE = 10              # fewer valid sales than this: is_low_sample (presentation choice)
MIN_SALES_FOR_MEDIAN = 5     # fewer than this: no median shown (presentation choice)

GOLD = "gold.valid_sale_prices_by_municipality_year"
STAGE = {"gold": "gold._stage_valid_sale_prices_by_municipality_year"}
LIVE = {"gold": GOLD}
OBSERVATIONS = "_gold_observations"
SHARED_SALE_DATES = "_gold_shared_sale_dates"


def stage_observations(con):
    # Parcels with valid rows on one sale date under two or more real deeds (03 §4.4)
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE {SHARED_SALE_DATES} AS
        SELECT parcel_id, sale_date, true AS on_several_deeds
        FROM {SILVER['records']}
        WHERE sale_validity_group = 'valid' AND NOT is_deed_placeholder
        GROUP BY 1, 2 HAVING count(DISTINCT (deed_book, deed_page)) > 1
    """)
    con.execute(f"""
        CREATE OR REPLACE TEMP TABLE {OBSERVATIONS} AS
        SELECT r.deed_book, r.deed_page,
               any_value(r.parcel_id) AS parcel_id,   -- one parcel, one date: single values
               any_value(r.sale_date) AS sale_date,
               any_value(r.sale_year) AS sale_year,
               any_value(r.muni_code) AS muni_code,
               min(r.price) AS price,
               count(*) AS valid_rows,
               count(DISTINCT r.price) AS prices
        FROM {SILVER['records']} r
        JOIN {SILVER['deeds']} d USING (deed_book, deed_page)
        ANTI JOIN {SHARED_SALE_DATES} x USING (parcel_id, sale_date)
        WHERE r.sale_validity_group = 'valid' AND r.price IS NOT NULL
          AND d.parcel_count = 1 AND d.sale_date_count = 1
        GROUP BY 1, 2
    """)


def stage_gold(con):
    con.execute("CREATE SCHEMA IF NOT EXISTS gold")
    con.execute(f"""
        CREATE OR REPLACE TABLE {STAGE['gold']} AS
        WITH recs AS (
            SELECT r.*, d.parcel_count, d.sale_date_count, coalesce(x.on_several_deeds, false) AS on_several_deeds
            FROM {SILVER['records']} r
            LEFT JOIN {SILVER['deeds']} d USING (deed_book, deed_page)
            LEFT JOIN {SHARED_SALE_DATES} x USING (parcel_id, sale_date)
        ),
        cells AS (
            SELECT sale_year, muni_code,
                   any_value(muni_desc) AS muni_desc,    -- one name per code (03 §4.5)
                   count(*) AS records_total,
                   count(*) FILTER (WHERE sale_validity_group = 'valid') AS records_valid,
                   count(*) FILTER (WHERE sale_validity_group = 'undetermined') AS records_undetermined,
                   -- counted on their own, not as valid minus used, so the reconciliation check can fail
                   count(*) FILTER (WHERE sale_validity_group = 'valid' AND (
                       is_deed_placeholder OR parcel_count > 1 OR sale_date_count > 1 OR price IS NULL
                       OR on_several_deeds
                   )) AS valid_records_excluded
            FROM recs GROUP BY 1, 2
        ),
        sales AS (
            SELECT sale_year, muni_code, count(*) AS n,
                   median(price) AS median_price,
                   quantile_cont(price, 0.25) AS p25_price,
                   quantile_cont(price, 0.75) AS p75_price
            FROM {OBSERVATIONS} GROUP BY 1, 2
        ),
        latest AS (SELECT max(sale_year) AS sale_year, max(sale_date) AS sale_date FROM {SILVER['records']})
        SELECT c.sale_year, c.muni_code, c.muni_desc,
               coalesce(s.n, 0) AS valid_sales_used,
               CASE WHEN coalesce(s.n, 0) >= {MIN_SALES_FOR_MEDIAN} THEN round(s.median_price)::BIGINT END AS median_price,
               CASE WHEN coalesce(s.n, 0) >= {MIN_SALES_FOR_MEDIAN} THEN round(s.p25_price)::BIGINT END AS p25_price,
               CASE WHEN coalesce(s.n, 0) >= {MIN_SALES_FOR_MEDIAN} THEN round(s.p75_price)::BIGINT END AS p75_price,
               c.records_total,
               c.records_valid,
               c.records_undetermined,
               round(100.0 * c.records_valid / c.records_total, 1) AS pct_valid,
               round(100.0 * c.records_undetermined / c.records_total, 1) AS pct_undetermined,
               c.valid_records_excluded,
               c.sale_year = l.sale_year AND l.sale_date < make_date(l.sale_year, 12, 31) AS is_partial_year,
               c.sale_year = l.sale_year AS is_provisional,
               coalesce(s.n, 0) < {LOW_SAMPLE} AS is_low_sample
        FROM cells c LEFT JOIN sales s USING (sale_year, muni_code) CROSS JOIN latest l
        ORDER BY c.sale_year, c.muni_code
    """)


def build_gold(con, build_id=None):
    """Rebuild the gold table from silver. Raises CheckFailed (after logging) if a hard check
    fails; the live gold table is then left as it was."""
    build_id = build_id or uuid.uuid4().hex
    started_at = now()
    versions = active_versions(con)
    drop_stage(con, STAGE)
    try:
        stage_observations(con)
        stage_gold(con)
        run_checks(con, GOLD_CHECKS,
                   {"gold": STAGE["gold"], "current_gold": GOLD, "records": SILVER["records"],
                    "observations": OBSERVATIONS},
                   "gold", build_id, values={"min_sales_for_median": MIN_SALES_FOR_MEDIAN})
        cells, used = con.execute(
            f"SELECT count(*), sum(valid_sales_used) FROM {STAGE['gold']}").fetchall()[0]
        swap_in(con, STAGE, LIVE)
    except Exception as exc:
        drop_stage(con, STAGE)
        log_build(con, build_id, "gold", "failed", versions, None, None, None, str(exc)[:1000], started_at)
        raise
    finally:
        con.execute(f"DROP TABLE IF EXISTS {OBSERVATIONS}")
        con.execute(f"DROP TABLE IF EXISTS {SHARED_SALE_DATES}")
    log_build(con, build_id, "gold", "ok", versions, None, cells, None, None, started_at)
    print(f"  gold: {cells:,} municipality-years from {used:,} valid single-parcel sales")
    return build_id
