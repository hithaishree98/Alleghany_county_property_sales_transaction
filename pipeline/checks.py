"""Data quality checks for the silver and gold builds.

Every check is one SQL query that returns how many rows (or years, or cells) fail it.
Each one has a severity:

    hard   the build stops and the previous silver/gold stays live. Used for things that
           mean the source contract broke or our own code is wrong.
    warn   reported in meta.check_results and printed; the build continues. Used for drift
           that a person should look at.
    info   counted every build so changes are visible; never a failure. Used for known
           limitations of the source data (they stay in silver with a flag).

`{name}` placeholders in the SQL are filled with table names when the checks run, so the
same checks run on the staged tables of a build and on hand-made tables in the tests.
"""

from dataclasses import dataclass

from pipeline.ingest import now

EXPECTED_COLUMNS = [
    "_id", "PARID", "FULL_ADDRESS", "PROPERTYHOUSENUM", "PROPERTYFRACTION", "PROPERTYADDRESSDIR",
    "PROPERTYADDRESSSTREET", "PROPERTYADDRESSSUF", "PROPERTYADDRESSUNITDESC", "PROPERTYUNITNO",
    "PROPERTYCITY", "PROPERTYSTATE", "PROPERTYZIP", "SCHOOLCODE", "SCHOOLDESC", "MUNICODE", "MUNIDESC",
    "RECORDDATE", "SALEDATE", "PRICE", "DEEDBOOK", "DEEDPAGE", "SALECODE", "SALEDESC", "INSTRTYP",
    "INSTRTYPDESC",
]

# A year whose rejected rows go above this share is a format change, not a few bad rows.
MAX_REJECT_RATE = 0.001
# A past year losing more than this share of its rows between two builds needs a look.
MAX_YEAR_DROP = 0.05
# The first year file; a sale date before it, or after the file's last_modified date, is not a real date.
FIRST_SALE_DATE = "2012-01-01"
# A valid-coded sale below this price is reported; the sale code still decides.
MIN_VALID_PRICE = 1000

SETUP_SQL = """
CREATE SCHEMA IF NOT EXISTS meta;
CREATE TABLE IF NOT EXISTS meta.check_results (
    build_id     VARCHAR,
    layer        VARCHAR,
    check_name   VARCHAR,
    severity     VARCHAR,
    failing      BIGINT,
    passed       BOOLEAN,
    about        VARCHAR,
    checked_at   TIMESTAMPTZ
);
"""


@dataclass
class Check:
    name: str
    severity: str        # hard | warn | info
    sql: str             # returns one number: how many rows/years/cells fail
    about: str           # what it guards and why (the finding behind it)
    needs: tuple = ()    # tables that must exist for the check to run (e.g. the previous build)


class CheckFailed(RuntimeError):
    """One or more hard checks failed; the build did not replace the live tables."""


# ---------- silver ----------

SILVER_CHECKS = [
    # source contract and our own code: these stop the build
    Check("silver_rows_reconcile", "hard", """
        SELECT count(*) FROM (
            SELECT source_year, count(*) AS n FROM {source} GROUP BY 1) b
        FULL JOIN (
            SELECT source_year, sum(n) AS n FROM (
                SELECT source_year, count(*) AS n FROM {records} GROUP BY 1
                UNION ALL SELECT source_year, count(*) FROM {rejected} GROUP BY 1) GROUP BY 1) s
        USING (source_year)
        WHERE b.n IS DISTINCT FROM s.n""",
          "Years where silver rows + rejected rows differ from the bronze rows. Silver must never "
          "lose or duplicate a record (01 §1.1)."),
    Check("bronze_matches_manifest", "hard", """
        SELECT count(*) FROM (SELECT source_year, source_version, count(*) AS n FROM {source} GROUP BY 1, 2) b
        JOIN meta.load_manifest m ON m.year = b.source_year AND m.version = b.source_version
        WHERE m.loaded_row_count <> b.n""",
          "Years whose active bronze table no longer has the row count the pipeline recorded when "
          "it loaded it (01 §1.0)."),
    Check("reject_rate_per_year", "hard", f"""
        SELECT count(*) FROM (SELECT source_year, count(*) AS n FROM {{source}} GROUP BY 1) b
        JOIN (SELECT source_year, count(*) AS n FROM {{rejected}} GROUP BY 1) r USING (source_year)
        WHERE r.n > {MAX_REJECT_RATE} * b.n""",
          f"Dataset-specific: years where more than {MAX_REJECT_RATE:.1%} of rows could not be parsed. "
          "Dates are read as year-month-day because that is what every file uses, although the data "
          "dictionary says dd/mm/yyyy (02 §3.2). If a file switched to the documented format, most of its "
          "dates would fail to parse; that is a format change, not a few bad rows, so the build stops "
          "instead of loading a half-read year."),
    Check("required_fields_present", "hard", """
        SELECT count(*) FROM {records} WHERE parcel_id IS NULL OR sale_date IS NULL OR record_hash IS NULL""",
          "Rows in silver without a parcel, a sale date or a record hash. Rows like that should have "
          "been rejected, so this would be a bug in the transform."),
    Check("sale_date_in_range", "hard", f"""
        SELECT count(*) FROM {{records}} r
        JOIN meta.load_manifest m ON m.year = r.source_year AND m.version = r.source_version
        WHERE r.sale_date NOT BETWEEN DATE '{FIRST_SALE_DATE}' AND left(m.source_last_modified, 10)::DATE""",
          f"Rows in silver with a sale date before {FIRST_SALE_DATE} or after their file was last modified. "
          "Rows like that are rejected (0 today), so this would be a bug in the transform."),
    Check("no_placeholder_deed_groups", "hard", """
        SELECT count(*) FROM {deeds}
        WHERE deed_book IS NULL OR deed_page IS NULL OR trim(deed_book, '0 ') = '' OR trim(deed_page, '0 ') = ''""",
          "Dataset-specific: blank or zero deed references (0/0, 00/00, ...) are not documents; 0/0 alone "
          "is shared by 957 unrelated parcels (01 §2.4). If one became a deed group, every parcel on it "
          "would look like one multi-parcel deed."),
    Check("record_hash_unique", "hard", """
        SELECT count(*) - count(DISTINCT (record_hash, record_seq)) FROM {records}""",
          "The silver row identifier (record_hash, record_seq) must be unique."),

    # drift: reported, the build continues
    Check("sale_date_outside_file_year", "warn", """
        SELECT count(*) FROM {records} WHERE sale_year <> source_year""",
          "Every yearly file holds only sale dates of its own year (01 §1.2). A row outside it means the "
          "source changed how it splits the files, and the same transfer could then sit in two files."),
    Check("rejected_rows", "warn", "SELECT count(*) FROM {rejected}",
          "Rows that could not be parsed (blank or malformed parcel, sale date, price or record date). "
          "They are kept in silver.rejected_records with the reason; 0 today."),
    Check("exact_duplicate_rows", "warn", "SELECT count(*) FROM {records} WHERE record_seq > 1",
          "Rows identical to another row in every column except _id. 0 today (03 §4.4); kept, numbered "
          "with record_seq."),
    Check("sale_code_not_in_lookup", "warn", "SELECT count(*) FROM {records} WHERE sale_validity_group IS NULL",
          "Sale codes missing from pipeline/ref/sale_validation_codes.csv (02 §3.4). Their rows are never "
          "counted as valid sales."),
    Check("code_description_not_one_to_one", "warn", """
        SELECT (SELECT count(*) FROM (SELECT sale_code FROM {records} GROUP BY 1 HAVING count(DISTINCT sale_desc) > 1))
             + (SELECT count(*) FROM (SELECT muni_code FROM {records} GROUP BY 1 HAVING count(DISTINCT muni_desc) > 1))
             + (SELECT count(*) FROM (SELECT school_code FROM {records} GROUP BY 1 HAVING count(DISTINCT school_desc) > 1))
             + (SELECT count(*) FROM (SELECT instrument_type FROM {records} GROUP BY 1 HAVING count(DISTINCT instrument_desc) > 1))""",
          "Codes with more than one description. Today every code has one (03 §4.5); grouping uses codes."),
    Check("code_format", "warn", """
        SELECT count(*) FROM {records}
        WHERE NOT regexp_matches(school_code, '^[0-9]{{2}}$') OR NOT regexp_matches(muni_code, '^[0-9]{{3}}$')
           OR NOT regexp_matches(instrument_type, '^[A-Z]{{2}}$') OR NOT regexp_matches(coalesce(zip, '00000'), '^[0-9]{{5}}$')
           OR NOT regexp_matches(coalesce(house_number, '0'), '^[0-9]+$')""",
          "School 2 digits, municipality 3 digits, instrument 2 letters, ZIP 5 digits, house number digits "
          "(02 §3.4–3.5, 0 exceptions today). A new shape is drift, not a reason to stop."),
    Check("year_lost_rows", "warn", f"""
        SELECT count(*) FROM (SELECT source_year, count(*) AS n FROM {{current_records}} GROUP BY 1) old
        JOIN (SELECT source_year, count(*) AS n FROM {{records}} GROUP BY 1) new USING (source_year)
        WHERE new.n < (1 - {MAX_YEAR_DROP}) * old.n""",
          f"Years that lost more than {MAX_YEAR_DROP:.0%} of their rows since the last build. Real source "
          "changes so far removed at most 206 rows (01 §1.3).",
          needs=("current_records",)),

    # known source limitations: counted, never a failure
    Check("placeholder_deed_rows", "info", "SELECT count(*) FROM {records} WHERE is_deed_placeholder",
          "Rows with a blank or zero deed reference (2,948 when analysed, 01 §2.4)."),
    Check("repeated_parcel_deed_date_rows", "info", "SELECT count(*) FROM {records} WHERE is_repeated_parcel_deed_date",
          "Rows in parcel + deed + sale date groups with more than one row (180 when analysed, 01 §2.5)."),
    Check("record_date_before_sale_date", "info", "SELECT count(*) FROM {records} WHERE is_record_before_sale",
          "Recorded before the sale happened (733 when analysed, 03 §4.1)."),
    Check("record_date_out_of_range", "info", "SELECT count(*) FROM {records} WHERE is_record_date_out_of_range",
          "Record dates before 2012 or after the file's last-modified date (4 when analysed, 02 §3.2)."),
    Check("record_date_blank", "info", "SELECT count(*) FROM {records} WHERE record_date IS NULL",
          "Blank record dates (1,249 when analysed, 02 §3.2)."),
    Check("price_blank", "info", "SELECT count(*) FROM {records} WHERE price IS NULL",
          "Blank prices (3,108 when analysed, 02 §3.3)."),
    Check("price_10m_or_more", "info", "SELECT count(*) FROM {records} WHERE price >= 10000000",
          "Prices of $10M or more (402 when analysed, 02 §3.3); usually deed totals."),
    Check("unclassified_sale_code_rows", "info",
          "SELECT count(*) FROM {records} WHERE sale_validity_group = 'unclassified'",
          "Rows with sale code PV, not in the data dictionary (33 when analysed)."),
    Check("deed_references_in_several_years", "info", "SELECT count(*) FROM {deeds} WHERE sale_year_count > 1",
          "Deed references that appear in more than one year file (293 when analysed, 03 §4.7 D)."),
]


# ---------- gold ----------

GOLD_CHECKS = [
    Check("gold_one_row_per_cell", "hard", """
        SELECT count(*) - count(DISTINCT (sale_year, muni_code)) FROM {gold}""",
          "One row per sale year and municipality."),
    Check("gold_valid_rows_reconcile", "hard", """
        SELECT abs((SELECT coalesce(sum(valid_sales_used + valid_records_excluded), 0) FROM {gold})
                 - (SELECT count(*) FROM {records} WHERE sale_validity_group = 'valid'))""",
          "Dataset-specific: every valid-coded silver row is either used once or counted as excluded. "
          "Multi-parcel deeds repeat the deed's total price on every parcel (03 §4.3), so a row used "
          "twice would count the same money twice."),
    Check("gold_one_valid_row_per_sale", "hard", """
        SELECT count(*) FROM {observations} WHERE valid_rows > 1 OR prices > 1""",
          "Each sale used in gold has exactly one valid row and one price. True today (03 §4.7 B, E); "
          "if it breaks, two rows describe one sale and a rule to choose between them is needed."),
    Check("gold_one_sale_per_parcel_date", "hard", """
        SELECT count(*) - count(DISTINCT (parcel_id, sale_date)) FROM {observations}""",
          "Dataset-specific: no parcel is used twice on one sale date. 60 parcels have valid-coded rows "
          "on one sale date under two different deeds, each covering one parcel (03 §4.4); those "
          "are excluded and counted in valid_records_excluded, so a sale is never counted twice."),
    Check("gold_medians_only_with_enough_sales", "hard", """
        SELECT count(*) FROM {gold}
        WHERE (median_price IS NOT NULL AND valid_sales_used < {min_sales_for_median})
           OR (median_price IS NULL AND valid_sales_used >= {min_sales_for_median})""",
          "A median is shown only when enough sales stand behind it."),
    Check("gold_shares_in_range", "hard", """
        SELECT count(*) FROM {gold}
        WHERE pct_valid NOT BETWEEN 0 AND 100 OR pct_undetermined NOT BETWEEN 0 AND 100""",
          "Coverage shares are percentages."),
    Check("gold_past_year_lost_sales", "warn", f"""
        SELECT count(*) FROM (SELECT sale_year, sum(valid_sales_used) AS n FROM {{current_gold}} GROUP BY 1) old
        JOIN (SELECT sale_year, sum(valid_sales_used) AS n FROM {{gold}} GROUP BY 1) new USING (sale_year)
        WHERE new.sale_year < (SELECT max(sale_year) FROM {{gold}}) AND new.n < (1 - {MAX_YEAR_DROP}) * old.n""",
          f"Past years whose valid sales dropped more than {MAX_YEAR_DROP:.0%} since the last build.",
          needs=("current_gold",)),
    Check("valid_sale_price_floor", "warn", f"""
        SELECT count(*) FROM {{observations}} WHERE price < {MIN_VALID_PRICE}""",
          f"Dataset-specific: valid-coded sales used in gold with a price under ${MIN_VALID_PRICE:,} "
          "(54 when added; 59 valid-coded rows under $1,000, 14 at $1 or less, 03 §4.7 A). Most "
          "records under $1,000 are family transfers; the sale code decides, so these stay in the "
          "medians until the County says whether they are errors."),
    Check("gold_low_sample_cells", "info", "SELECT count(*) FROM {gold} WHERE is_low_sample",
          "Cells with fewer than 10 valid sales, including municipality-years with none (when analysed: "
          "665 of the 2,489 cells with sales, 03 §4.7 E, plus 129 with no valid sale)."),
]


# ---------- runner ----------

def table_exists(con, qualified):
    schema, _, table = qualified.partition(".")
    return bool(con.execute(
        "SELECT 1 FROM duckdb_tables() WHERE schema_name = ? AND table_name = ? "
        "UNION ALL SELECT 1 FROM duckdb_views() WHERE schema_name = ? AND view_name = ?",
        [schema, table, schema, table]).fetchall())


def run_checks(con, checks, tables, layer, build_id, values=None):
    """Run every check against the given tables, record the results, print what failed.

    Raises CheckFailed after recording everything if any hard check failed, so all
    problems of one build are visible at once."""
    con.execute(SETUP_SQL)
    names = {**tables, **(values or {})}
    results = []
    for check in checks:
        if any(not table_exists(con, tables.get(n, "")) for n in check.needs):
            continue  # e.g. no previous build to compare with yet
        failing = con.execute(check.sql.format(**names)).fetchall()[0][0] or 0
        passed = check.severity == "info" or failing == 0
        results.append((check, failing, passed))
        con.execute("INSERT INTO meta.check_results VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                    [build_id, layer, check.name, check.severity, failing, passed, check.about, now()])
        if not passed:
            print(f"  {check.severity.upper():<4} {check.name}: {failing}")
    hard = [f"{c.name} ({n})" for c, n, ok in results if c.severity == "hard" and not ok]
    if hard:
        raise CheckFailed(f"{layer}: hard checks failed: {', '.join(hard)}")
    return results
