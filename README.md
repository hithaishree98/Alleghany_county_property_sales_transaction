# Allegheny County property sales pipeline

This project downloads the **Allegheny County Property Sale Transactions** data from WPRDC, keeps the raw files exactly as downloaded, cleans the data, and builds one output table and a dashboard from it that answers a question about sale prices.

## Contents

1. [How to run it](#how-to-run-it)
2. [How the layers are designed](#how-the-layers-are-designed)
3. [The question and the output table](#the-question-and-the-output-table)
4. [Dashboard](#dashboard)
5. [Re-runs and source updates](#re-runs-and-source-updates)
6. [Data quality checks](#data-quality-checks)
7. [The three most important design decisions](#the-three-most-important-design-decisions)
8. [Daily refreshes and 100x scale](#daily-refreshes-and-100x-scale)
9. [AI disclosure](#ai-disclosure)
10. [Appendix: the `meta.*` tables](#appendix-the-meta-tables)
11. [What each part does](#what-each-part-does)

## How to run it

You need Python 3.11. From the project folder:

```
pip install -r requirements.txt

python -m pipeline.run                  # download all years, then build silver and gold
python -m pipeline.run --skip-ingest    # rebuild silver and gold from the files already downloaded
python -m pipeline.dashboard            # rebuild only the dashboard from the current gold table
python -m pytest -q                     # run the tests (no download needed)
```

Everything is written to `data/`: the raw files go to `data/landing/` and the tables to `data/warehouse.duckdb`. Every successful run ends by rewriting the dashboard, `dashboard/home_sale_prices.html` (open it in a browser), and the gold table as `dashboard/valid_sale_prices_by_municipality_year.csv`.

## How the layers are designed

The data moves through four layers: landing → bronze → silver → gold. Each layer is rebuilt from the one before it, so I can always go back to the raw files.

![Pipeline layers](image.png)

## The question and the output table

**Question:** what is the typical price paid for a property in each municipality, each year, counting only sales the County codes as valid, of a single parcel?

### What the table shows

- **Prices rose slowly until 2019 (1.12x from 2013 to 2019, about 2% a year), jumped 24% in 2020 and 12% in 2021, and have grown 4–8% a year since (2022–2025).** In 2020, 52 of 53 municipalities (98%) went up, so it wasn't a few outliers.
- **Every municipality I could measure went up after 2019.** I followed the same 53 municipalities with 30+ sales in 2013, 2019 and 2025. The typical one grew 1.12x from 2013 to 2019 and 1.72x from 2019 to 2025, and all 53 were higher in 2025 than in 2019.
- **Cheaper places went up the most.** I took the 53 municipalities with 30+ sales in both 2019 and 2025, split them into four groups by their 2019 price, and compared each place's 2025 price with its own 2019 price. The cheapest group's typical place grew 2.15x and the most expensive group's 1.46x, and growth fell steadily from cheapest to priciest. Most of the gap opened in 2020–2021.
- **Expensive places stay expensive, but the gap is shrinking.** Each year I ranked municipalities by their median price; the order hardly changes from one year to the next. But the place near the top (90th percentile) cost about 3.7x the place near the bottom (10th percentile) in 2019 and only 2.7x in 2025: cheap places more than doubled while expensive ones rose about 60%.

## Dashboard

`dashboard/home_sale_prices.html` is a single page for people who use the County's data rather than the code.

| Tab                  | What it shows                                                                                                                                                                                                                      |
| -------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Home                 | What the page is, the headline numbers for the last complete year, the findings above written out with the current numbers, and a short caveat                                                                                     |
| Data and Definitions | What data is available, what each term means, the limits (2012, 2019, the provisional year, no property type), and how much of each year is coded valid                                                                            |
| County Summary       | Typical yearly change, share of places going up, the spread between expensive and cheap places, growth by price quarter, before/after the jump, and the highest and lowest places for any year                                     |
| Municipality         | A map of the typical price by municipality and ward for any year, and a profile of one place compared with up to five others: price with its middle half, sales behind each year, and a year-by-year table with reliability badges |

## Re-runs and source updates

Every run (`pipeline/run.py`) checks each year's file, keeps or adds a bronze version, then rebuilds silver, gold and the dashboard. A bad, duplicate or half-written file can never replace what's already live.

### For each of the 15 year files (`ingest.py`, using `ckan.py` to talk to the API)

1. **I download every year again.** The source also changes past years' files, so checking only the latest year would miss changes.
2. **I check the source isn't in the middle of rewriting the file.** Before and after the download, I read the file's last-modified time, row count and columns from the API. If I've already seen that last-modified time with a different row count, or anything changed during the download, I refuse the file: the year keeps its current version, the failure goes into `run_log`, and the next run tries again.
3. **The file is saved to landing unchanged**, as `data/landing/real_estate_sales/<year>/<sha256>.csv.gz`. It's named by the SHA-256 of its original bytes, so the same content is never stored twice, and gzip uses a fixed timestamp, so the same download always gives the same file. A refused file stays in landing too, but is never used.
4. **I compare it with what I already have.** A file I haven't seen before is first parsed and checked: the header must equal the API's columns, every row must have every field, and the row count must equal the API's. A mismatch is refused, as in step 2.

| What I find                                                 | Result                                                                                                                        | Meta tables updated                                                  |
| ----------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------- |
| Same SHA-256 as the version in use                          | `identical_file` → nothing to do, after checking its bronze table still matches its manifest (if not, the year fails)         | `run_log`                                                            |
| Different SHA-256, same rows (ignoring `_id` and row order) | `republished_same_content` → no new version                                                                                   | `landing_alias`, `run_log`                                           |
| The file or its rows match an older version                 | `reverted` → switch back to that version, rebuilding it from landing first if its table was dropped                           | `active_version`, `load_manifest` (if rebuilt), `run_log`            |
| The rows really changed                                     | `content_changed` → new bronze version `sales_<year>_v<N+1>`, the view points to it, and the added and removed rows are saved | `load_manifest`, `active_version`, `change_log`, `diff_*`, `run_log` |
| First load of a year                                        | `new` → `sales_<year>_v1`                                                                                                     | `load_manifest`, `active_version`, `run_log`                         |

**The same file downloaded again** (a rerun with nothing new, or the source publishing a file I've already seen): it has the same SHA-256, so landing keeps the one copy it already has, and I recognise it without reading it again, whether it's the version in use, an older version, or an earlier re-publish remembered in `landing_alias`. Nothing new is created; only a `run_log` row says it was checked.

5. **Old bronze tables are dropped only if they can be rebuilt.** I keep the newest 2 versions of each year (`--keep-versions`). Older tables are dropped to save space, never the one in use, and only if their landing file still matches its SHA-256. `python -m pipeline.ingest --restore YEAR:VERSION` rebuilds one.

**Example, different file but same rows:** the source re-publishes 2018 with exactly the same sales, but every row gets a new `_id`. The bytes change → new SHA-256 → but the rows are the same → no new version. The file is remembered in `landing_alias`, so if it shows up again it's recognised by its SHA-256 without being read again.

**Example, `reverted`:** 2018 is on v1 → the source changes some rows → v2 becomes the version in use → later the source publishes the old content again → it matches v1 → the view `bronze.sales_2018` points back to v1. No new version is created. If v1's table had been dropped to save space (2 versions are kept per year), it's rebuilt from its landing file first.

### After all years (`silver.py`, `gold.py`, `checks.py`, `dashboard.py`)

6. **Silver and gold are rebuilt in full from the versions in use, never patched.** Each is built into staging tables, checked, and swapped in with one transaction only if no hard check fails; if one does, the live tables stay as they were. Silver also stops before building if there are no versions in use or a year doesn't have the 26 expected columns in order. Every build is logged in `build_log` and `check_results`.
7. **If gold fails, silver is switched back.** Silver is swapped in before gold is built, so if anything goes wrong in gold, silver goes back to the tables it replaced (logged as `rolled_back`). On the very first build there's nothing to go back to, so the new silver is dropped.
8. **The dashboard is rebuilt last,** only after silver and gold succeed. If it fails, a warning is printed and the previous page stays; the run doesn't fail because of it.

**How reruns and source updates reach silver, gold and the dashboard**

- **A rerun with nothing new** → the same bronze versions are in use, so silver and gold are rebuilt into exactly the same tables (tested in `tests/test_pipeline.py`). The dashboard is rewritten with the same numbers; only its "Refreshed" time changes.
- **The source updates a year** → its new bronze version is in use, so the whole of silver is rebuilt with it, gold is rebuilt from that silver, and the dashboard and its CSV are rewritten from that gold, all in the same run. Nothing is patched row by row, so the update reaches every table, every year it affects, and every chart.
- **The source reverts a year** → the older version is back in use, and silver, gold and the dashboard follow it on the same run.
- **I change my own silver or gold code** → the next run applies it to every year, since everything is rebuilt from bronze, and the dashboard shows the new numbers.
- **A build fails** → silver and gold stay on the last good build, and the dashboard isn't rebuilt, so the page keeps showing the numbers of that last good build.
- **Bronze has a new version but silver fails** → silver and gold are tables copied at build time, not views of bronze, so they keep the rows of the older version (each silver row's `source_version` and `build_log.source_versions` show which) and catch up on the next build that passes.
- **Each build is compared with the one before:** a warning if a year lost more than 5% of its rows in silver, or a past year lost more than 5% of its valid sales in gold.

**Result**

- **Running it twice** → no new version is stored, and silver and gold come out the same.
- **The source updates a year** → a new bronze version → silver, gold and the dashboard pick it up in the same run.
- **One year fails, for any reason** → it keeps its previous version, and the other years carry on.
- **Exit code** → if any year failed or a hard check stopped the build, a run ends with 1 else with 0.
- **A crash mid-run** → each layer is built in temporary tables and only swapped into the live ones inside one transaction, so a crash leaves the old tables intact, and the next run deletes the leftover temporary tables and .part files before starting.

## Data quality checks

Each check is one SQL query in `pipeline/checks.py` that counts the rows failing it. Silver and gold are built into staging tables → the checks run → the tables are swapped in only if no hard check fails. Every result is saved in `meta.check_results`.

### How the check types are designed

| Type       | Used for                                                                                                                           | What happens                                                                                                                                         | Why this level                                                                            |
| ---------- | ---------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------- |
| **hard**   | the source changed its format (a column renamed, dates in a new format), or my own code is wrong (rows lost, a sale counted twice) | the build stops, the last good silver and gold stay in place, and the run ends with an error                                                         | the numbers would be wrong, so publishing nothing new is safer than publishing them       |
| **reject** | a single row can't be read (bad parcel id, sale date, price or record date, or a sale date before 2012 or after the file's date)   | the row goes to `silver.rejected_records` with the reason, and the build goes on. If more than 0.1% of a year is rejected, it becomes a hard failure | one bad row shouldn't block 500,000 good ones, but many bad rows means the format changed |
| **warn**   | something changed that a person should look at (a new sale code, a new code format, a year losing more than 5% of its rows)        | printed and saved; the build goes on                                                                                                                 | it may be a real source change or a problem, and only a person can tell which             |
| **info**   | known problems in the source (placeholder deeds, record date before sale date)                                                     | counted on every build; the rows stay in silver with a flag                                                                                          | they're known and handled; counting them shows if they suddenly grow                      |

### The most important checks, from my profiling

Each of these exists because of something I found in the data. The section in brackets is where the analysis is (`02 §3.2` = notebook 02, section 3.2).

| Check                                         | What I found                                                                                                                                                                                         | Type |
| --------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---- |
| `reject_rate_per_year`                        | The data dictionary says dates are dd/mm/yyyy, but every file uses yyyy-mm-dd (02 §3.2). If a file switched to the documented format, most of its dates would fail to read, and this stops the build | hard |
| `no_placeholder_deed_groups`                  | Deed `0`/`0` is shared by 957 unrelated parcels (01 §2.4). Grouped as one deed, they'd all look like one multi-parcel sale                                                                           | hard |
| `gold_valid_rows_reconcile`                   | A deed with several parcels repeats the deed's total price on every parcel (03 §4.3). Every valid-coded row must be used once or counted as left out, so the same money is never counted twice       | hard |
| `gold_one_sale_per_parcel_date`               | 60 parcels have valid-coded rows on one sale date under two different deeds (03 §4.4). Neither is used, and this makes sure no parcel is counted twice on one date                                   | hard |
| `sale_code_not_in_lookup`                     | The County's sale codes seems to have changed over the years (02 §3.4). A new code is never counted as a valid sale until I add it to the lookup                                                     | warn |
| `year_lost_rows`, `gold_past_year_lost_sales` | Past years keep changing after they're published, with rows added and removed (01 §1.3)                                                                                                              | warn |
| `code_format`                                 | Codes keep their leading zeros and fixed lengths (02 §3.4–3.5). A new shape means the source changed how it writes them                                                                              | warn |

### Standard checks

- `silver_rows_reconcile`: the row counts add up from bronze to silver (hard)
- `bronze_matches_manifest`: bronze still has the row count it was loaded with (hard)
- `required_fields_present`, `sale_date_in_range`: rows that should have been rejected never reach silver (hard)
- `record_hash_unique`: every silver row has a unique id (hard)
- `gold_one_row_per_cell`: one output row per year and municipality (hard)
- `gold_medians_only_with_enough_sales`: gold's numbers are within their rules (hard)

### Not hard yet: waiting on the confirmation

Some checks are grouped under warn or info only because I can't tell from the data alone whether the rows are wrong. Once the answers are confirmed with data owner, each can become hard, become a reject, or stay as it is.

| Check (type today)                                                     | Open question                                                                                         | Where     |
| ---------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------- | --------- |
| `valid_sale_price_floor` (warn)                                        | Are valid-coded sales under $1,000 errors? 54 are in the medians today, because the sale code decides | 03 §4.7 A |
| `sale_code_not_in_lookup` (warn), `unclassified_sale_code_rows` (info) | What does sale code `PV` mean, and is it a valid sale?                                                | 02 §3.4   |
| `record_date_before_sale_date` (info)                                  | Are the 733 sales recorded before they happened typing errors, and which date is right?               | 03 §4.1   |
| `record_date_out_of_range` (info)                                      | Are record dates like 0212-08-01 typos for 2012?                                                      | 02 §3.2   |
| `price_blank` (info)                                                   | Does a blank price mean something different from $0?                                                  | 02 §3.3   |
| `repeated_parcel_deed_date_rows` (info)                                | In repeated parcel + deed + date groups, which rows to drop?                                          | 01 §2.5   |
| `deed_references_in_several_years` (info)                              | When the same parcel and deed are in two year files, which sale date is right?                        | 03 §4.4   |

## The three most important design decisions

### 1. Keep full copies of each year's file, and a new copy only when the rows change

**Why**
The source gives me no way to load only what changed:

- there's no change feed and no "updated at" column
- past years get changed too, and deleted rows aren't reported
- the last-modified time changes every morning even when nothing changed (I saw the files being rebuilt daily)

**What I do**
Download all 15 files on every run → compare with the version in use → save a new bronze version only when the rows really changed. Every version is a full copy of the year, and silver and gold are always rebuilt from a full version, never patched.

**Why gzip in landing**

- 441 MB raw → 94 MB gzipped (4.7x smaller) for the 61 files kept so far. This matters even more in cloud storage, where every version is kept.
- It's still a plain CSV that any tool can read.
- The SHA-256 is taken before gzip, and gzip uses a fixed timestamp → the same download always gives the same file and name.

**How long things are kept**

- Landing files: forever. They're the record of exactly which file was used and when.
- Bronze tables: the newest versionfix 2,3 per year by default (`--keep-versions`). Older ones can be rebuilt from landing with `python -m pipeline.ingest --restore YEAR:VERSION`.

**Rejected alternatives**

- _Overwrite with the latest download_ → I'd lose what changed and when, and a half-written file could replace a good one.
- _Load only what changed_ → there's nothing reliable to base it on (no change feed, no updated-at, deletions not reported).
- _Skip the download if last-modified hasn't changed_ → it changes every day anyway, so it doesn't tell me anything.
- _Store only the differences instead of full copies_ → smaller, but getting a version back means replaying every difference. At this size full copies are simpler.
- _Store raw files uncompressed_ → 4.7x more disk for no benefit.

### 2. Detect changes by what's in the rows, not by `_id`

**Why**
The source gives every row an `_id`, but gives every row a new one each time it re-publishes a file, even when nothing changed. So a new file or a new `_id` doesn't mean new data, and `_id` can't identify a row. I keep it only to trace a row back to the file (`source_row_id` in silver), never as a key.

**Two hashes, two questions**

| Hash            | Of what                                 | Answers                          | Why I need it                                                |
| --------------- | --------------------------------------- | -------------------------------- | ------------------------------------------------------------ |
| SHA-256         | the file's original bytes               | "is this exactly the same file?" | quick, no need to read the file; also names the landing file |
| Row fingerprint | every row, ignoring `_id` and row order | "is this the same data?"         | the bytes change on every re-publish because `_id` changes   |

Same SHA-256 → stop. Different SHA-256 → check the row fingerprint → same → no new version. 35 re-publishes were caught this way.

**Rejected alternatives**

- _Match rows by `_id`_ → it changes on every re-publish.
- _SHA-256 only_ → every re-publish would become a new version (35 versions that aren't real changes).
- _Row fingerprint only_ → every download would have to be read in full, and landing files would have no content-based name to avoid storing duplicates.
- _Use parcel + deed + sale date as the row key and update rows in place_ → that combination repeats in 90 groups with conflicting rows, so an update would silently keep one of them.

### 3. Silver keeps every source record and flags problems instead of fixing them

**Why**
There's no reliable id for a sale. Parcel + deed + sale date looks like one, but it repeats in 90 groups where the sale code (77 groups) or the price (25 groups) disagree, and only the County can say which row is right.

**What silver does**

- **Types and trims** every row, and keeps it.
- **Flags** known problems: placeholder deed (`0`/`0`), repeated parcel + deed + date, record date before the sale date.
- **Sets aside** only rows that can't be read at all, in `silver.rejected_records` with the reason (0 today).
- **Maps each sale code** to valid / invalid / undetermined using the code, never the description.

**Rejected alternatives**

- _Keep one row per sale_ → there's no evidence-based rule for which row to keep.
- _Drop invalid sales in silver_ → whether a sale is valid is the County's call, and it changes over time (400 sale codes changed in 3 days), so that filter belongs in gold.
- _Remove leading zeros from deed pages_ (`00137` vs `137`) → it would merge only 6 deed numbers, and 4 of those are clearly different sales.
- _Fix data-entry mistakes_ (e.g. a record date in the year 212) → guessing the right value would hide the problem. They're kept as written and counted.

## Daily refreshes and 100x scale

The design stays the same only the infrastructure changes:

- Raw files go to S3 with versioning and object lock, so nothing can ever be overwritten or deleted.
  Why: raw files are the only proof of what the County published, and every version can be rebuilt from them.
- Each bronze version becomes a write-once Parquet folder in S3, one per year and version.
  Why: Parquet is compressed and fast to query, and write-once keeps every version exactly as it was loaded.
- Silver and gold become Iceberg tables. Their snapshot commit replaces my rename-in-a-transaction, and rollback fixes the crash gap between the two swaps.
  Why: readers always see a complete build, and a bad or half-finished one can be undone in one step.
- Airflow runs one DAG daily at 9 a.m. Eastern, after the source finishes its morning rebuild.
  Why: running earlier risks catching a half-written file, which I saw happen twice.
- DAG shape - Cleanup, then 15 ingest tasks in parallel, then silver, gold and the dashboard. Each year retries on its own, so one bad file never blocks the rest.
  Why: years are independent, so parallel tasks are faster, and isolated retries keep one bad file from delaying all the others.
- One Docker image, tested in CI. Each task runs it with a different command, with config from environment variables and secrets from a secrets manager.
  Why: every step runs the exact same tested code, and the same image works in dev and prod without leaking credentials.
- Hard failures page someone, warnings go to Slack, and a freshness alert fires if no run succeeds within a day.
  Why: the alert matches the urgency: a hard failure blocks publishing, a warning needs a look, and silence can hide a stuck pipeline.
- 100x is about 50 million rows, which DuckDB still handles on one machine, so no Spark needed.
  Why: a cluster adds cost and complexity that this data size doesn't need.
- 100x rebuilds: Rebuild silver only for years whose bronze changed (the change log already says which), and recompute gold only for affected sale years.
  Why: rebuild time then grows with how much changed, not with total history.
- Switch the CSV parser to streaming batches, since today it holds a whole file in memory.
  Why: memory is the first thing to break at 100x, and streaming keeps it flat without giving up strict parsing.
- An S3 lifecycle rule moves old raw files to cold storage: still kept, just cheaper.
  Why: old raw files are rarely read but must never be lost, and cold storage costs a fraction of standard.

## AI disclosure

I used **Claude Code** as a collaborative partner:

- **Understanding the data:** to understand the dataset and cross-check my own understanding of it.
- **Profiling plan:** I built the profiling steps and the plan together with Claude. I went through the plan it suggested, questioned the parts I wasn't convinced about or had doubts on, and changed them. Then I asked it to implement the plan with a few rules: follow the project's folder structure, reuse common code, and change an existing function instead of writing a new one when one already exists.
- **Checking my analysis:** to check my analysis and explanations against what each query actually returned. In some places it added analysis I hadn't thought of at first.
- **Open questions:** to check whether the official website or documentation had any information on my open questions, and to reason through them. Anything the source doesn't confirm, I left as an open question, since as a data engineer it needs to be confirmed, not assumed.
- **Design:** to think through the design and the check types. This was most useful when one of us suggested a design and we reasoned through what happens when the data grows or runs in production, and debated the alternatives until I reached a solution I found workable.
- **Writing:** to review this README and the notebooks for accuracy, grammar and wording.

## Appendix: the `meta.*` tables

| Table                   | One row =                                                                                 |
| ----------------------- | ----------------------------------------------------------------------------------------- |
| `load_manifest`         | one loaded version: landing file, SHA-256, row fingerprint, row count, last-modified time |
| `active_version`        | the version in use for each year                                                          |
| `landing_alias`         | a re-published file with the same rows as an existing version                             |
| `run_log`               | what happened to one year in one run                                                      |
| `change_log`            | one real change: year, from which version to which, rows added and removed                |
| `diff_<year>_v<a>_v<b>` | the exact rows added and removed in that change                                           |
| `build_log`             | one silver or gold build: ok or failed, which bronze versions it used                     |
| `check_results`         | one check in one build: how many rows failed, and whether it stopped the build            |

## What each part does

| File                                                        | What it does                                                                                                                                               |
| ----------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `pipeline/ckan.py`                                          | Finds the 15 yearly files (2012–2026) through the CKAN API, and reads each file's last-modified time, row count and columns                                |
| `pipeline/ingest.py`                                        | Downloads each year into landing. Loads it into bronze as a new version only if its rows really changed. Logs every run in `meta.*`                        |
| `pipeline/silver.py`                                        | Gives every column its proper type, trims spaces, flags known problems, sets aside rows that can't be read, and builds the deed table. No rows are removed |
| `pipeline/gold.py`                                          | Builds the output table: median, p25 and p75 price of valid single-parcel sales per year and municipality, and how many sales each number is based on      |
| `pipeline/checks.py`                                        | The data quality checks. **hard** stops the build, **warn** is reported, **info** is counted. Results go to `meta.check_results`                           |
| `pipeline/insights.py`                                      | The questions asked of the gold table, as SQL, shared by notebook 04 and the dashboard                                                                     |
| `pipeline/dashboard.py`, `pipeline/dashboard_template.html` | Writes the dashboard page and the CSV from gold, and builds the map outlines                                                                               |
| `pipeline/run.py`                                           | Runs ingest → silver → gold → dashboard                                                                                                                    |
| `notebooks/`                                                | 01–03: exploring the data, which shaped the design and the checks. 04: what the output table shows, with a chart per question                              |
| `tests/`                                                    | Break a small fake dataset on purpose and check that the pipeline stops, rejects or warns as expected                                                      |
