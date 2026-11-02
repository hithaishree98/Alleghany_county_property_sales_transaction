"""The questions asked of the gold table, as SQL: one place for notebook 04 and the dashboard.

Notebook 04 shows each query's result and what it means; the dashboard draws the same results.
Keeping the SQL here means the two can't drift apart (README, "The output table and the
question it answers", quotes the numbers).

    coverage_by_year        how much of each year is coded valid, and how many places compare
    typical_change          typical municipal median per year, and the within-municipality change
    quarter_growth          growth to the latest complete year, by price quarter at the start year
    before_after            growth before and after the jump year, for the same municipalities
    spread_and_rank         p10 / median / p90 of municipal medians, and rank stability
    highest_lowest          the top and bottom municipalities in one year
    one_municipality        every year of one municipality

Comparisons only use rows with at least MIN_SALES valid sales: a median of a handful of sales
jumps around (04 intro).
"""

G = "gold.valid_sale_prices_by_municipality_year"
MIN_SALES = 30           # valid sales a row needs to be used in comparisons
MIN_MUNICIPALITIES = 50  # comparable municipalities a year needs for its county level to be read


def overview():
    return f"""
SELECT count(*) AS rows, count(DISTINCT muni_code) AS municipalities,
       min(sale_year) AS first_year, max(sale_year) AS last_year,
       sum(records_total) AS records, sum(records_valid) AS valid_coded,
       sum(valid_records_excluded) AS valid_excluded, sum(valid_sales_used) AS valid_sales_used,
       count(median_price) AS rows_with_median, median(valid_sales_used) AS median_sales_per_row
FROM {G}
"""


def coverage_by_year():
    return f"""
SELECT sale_year,
       sum(records_total) AS records,
       sum(valid_sales_used) AS valid_sales_used,
       round(100.0 * sum(records_valid) / sum(records_total), 1) AS pct_valid,
       round(100.0 * sum(records_undetermined) / sum(records_total), 1) AS pct_undetermined,
       count(*) FILTER (WHERE valid_sales_used >= {MIN_SALES}) AS municipalities_comparable,
       count(*) FILTER (WHERE median_price IS NULL) AS rows_without_median,
       bool_or(is_partial_year) AS partial, bool_or(is_provisional) AS provisional
FROM {G} GROUP BY 1 ORDER BY 1
"""


def latest_complete_year(con):
    """The latest year whose numbers are final: the end point of every comparison."""
    return con.execute(f"SELECT max(sale_year) FROM {G} WHERE NOT is_provisional").fetchall()[0][0]


def typical_change():
    # The change is measured inside each municipality, so a different set of municipalities
    # in one year can't create a change on its own
    return f"""
WITH level AS (
    SELECT sale_year, count(*) AS municipalities, median(median_price) AS typical_median
    FROM {G} WHERE valid_sales_used >= {MIN_SALES} GROUP BY 1
), change AS (
    SELECT b.sale_year, count(*) AS municipalities_both_years,
           round(100 * (median(b.median_price * 1.0 / a.median_price) - 1), 1) AS typical_change_pct,
           round(100.0 * avg((b.median_price > a.median_price)::INT), 0) AS pct_municipalities_up
    FROM {G} a JOIN {G} b ON b.muni_code = a.muni_code AND b.sale_year = a.sale_year + 1
    WHERE a.valid_sales_used >= {MIN_SALES} AND b.valid_sales_used >= {MIN_SALES}
    GROUP BY 1
)
SELECT * FROM level LEFT JOIN change USING (sale_year) ORDER BY sale_year
"""


def jump_year(con):
    """The final year with the largest typical change, used to split the period (None when no
    two final years can be compared)."""
    found = con.execute(f"""
        SELECT b.sale_year
        FROM {G} a JOIN {G} b ON b.muni_code = a.muni_code AND b.sale_year = a.sale_year + 1
        WHERE a.valid_sales_used >= {MIN_SALES} AND b.valid_sales_used >= {MIN_SALES} AND NOT b.is_provisional
        GROUP BY 1 ORDER BY median(b.median_price * 1.0 / a.median_price) DESC LIMIT 1
    """).fetchall()
    return found[0][0] if found else None


def quarter_growth(latest):
    """One row per start year: median growth to `latest` of each price quarter (Q1 cheapest)."""
    return f"""
WITH pairs AS (
    SELECT a.sale_year AS start_year, a.muni_code, a.median_price AS start_median,
           b.median_price * 1.0 / a.median_price AS growth
    FROM {G} a JOIN {G} b ON b.muni_code = a.muni_code AND b.sale_year = {latest}
    WHERE a.sale_year < {latest} AND a.valid_sales_used >= {MIN_SALES} AND b.valid_sales_used >= {MIN_SALES}
), quartered AS (
    SELECT *, ntile(4) OVER (PARTITION BY start_year ORDER BY start_median) AS price_quarter FROM pairs
)
PIVOT (SELECT start_year, 'Q' || price_quarter AS q, growth FROM quartered)
ON q USING round(median(growth), 2)
ORDER BY start_year
"""


def before_after(jump, latest):
    """Growth up to the year before the jump, and from it to `latest`, for the municipalities
    comparable in all three years. Starts at the first year with MIN_MUNICIPALITIES to compare."""
    return f"""
WITH y AS (SELECT muni_code, sale_year, median_price AS p FROM {G} WHERE valid_sales_used >= {MIN_SALES}),
first_ AS (SELECT min(sale_year) AS y FROM y WHERE sale_year < {jump} - 1
           AND sale_year IN (SELECT sale_year FROM y GROUP BY 1 HAVING count(*) >= {MIN_MUNICIPALITIES}))
SELECT (SELECT y FROM first_) AS from_year, {jump} - 1 AS split_year, {latest} AS to_year,
       count(*) AS municipalities,
       round(median(b.p * 1.0 / a.p), 2) AS typical_growth_before,
       round(median(c.p * 1.0 / b.p), 2) AS typical_growth_after,
       count(*) FILTER (WHERE c.p > b.p) AS rose_after
FROM y a JOIN y b USING (muni_code) JOIN y c USING (muni_code)
WHERE a.sale_year = (SELECT y FROM first_) AND b.sale_year = {jump} - 1 AND c.sale_year = {latest}
"""


def spread_and_rank():
    return f"""
WITH ranked AS (
    SELECT sale_year, muni_code, median_price, rank() OVER (PARTITION BY sale_year ORDER BY median_price) AS r
    FROM {G} WHERE valid_sales_used >= {MIN_SALES}
), spread AS (
    SELECT sale_year, count(*) AS municipalities,
           quantile_cont(median_price, 0.1)::BIGINT AS p10_median, median(median_price)::BIGINT AS typical_median,
           quantile_cont(median_price, 0.9)::BIGINT AS p90_median,
           round(quantile_cont(median_price, 0.9) / quantile_cont(median_price, 0.1), 2) AS p90_to_p10
    FROM ranked GROUP BY 1
), order_kept AS (
    SELECT b.sale_year, round(corr(a.r, b.r), 2) AS rank_corr_with_previous_year
    FROM ranked a JOIN ranked b ON b.muni_code = a.muni_code AND b.sale_year = a.sale_year + 1
    GROUP BY 1
)
SELECT * FROM spread LEFT JOIN order_kept USING (sale_year) ORDER BY sale_year
"""


def highest_lowest(year, n=5):
    return f"""
(SELECT 'highest' AS end_, muni_desc, median_price, p25_price, p75_price, valid_sales_used FROM {G}
 WHERE sale_year = {year} AND valid_sales_used >= {MIN_SALES} ORDER BY median_price DESC LIMIT {n})
UNION ALL
(SELECT 'lowest', muni_desc, median_price, p25_price, p75_price, valid_sales_used FROM {G}
 WHERE sale_year = {year} AND valid_sales_used >= {MIN_SALES} ORDER BY median_price LIMIT {n})
"""


def most_sales_municipality(con):
    """The municipality with the most valid sales over all years."""
    return con.execute(
        f"SELECT muni_code FROM {G} GROUP BY 1 ORDER BY sum(valid_sales_used) DESC LIMIT 1").fetchall()[0][0]


def one_municipality(muni_code):
    return f"""
SELECT sale_year, muni_desc, median_price, p25_price, p75_price, valid_sales_used,
       pct_valid, pct_undetermined, is_low_sample, is_provisional
FROM {G} WHERE muni_code = '{muni_code}' ORDER BY sale_year
"""
