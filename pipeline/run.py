"""Local runner: calls the pipeline functions in order.

    1. ingest   download every year from WPRDC into landing and bronze (pipeline/ingest.py)
    2. silver   rebuild the cleaned table from the active bronze versions (pipeline/silver.py)
    3. gold     rebuild the output table from silver (pipeline/gold.py)

Scheduling, task retries, run history and alerting are an orchestrator's job (see README,
"Daily refreshes and 100x scale"); this runner only sequences the steps for a local run. A
failed year doesn't stop the other years. Silver and gold are rebuilt from whatever bronze
version is active, so a failed year simply keeps its previous version. If a hard check
fails, the live silver and gold tables stay as they were (a gold failure also switches
silver back) and the run exits non-zero.

Usage (from the project root):
    python -m pipeline.run [--years 2012 2013] [--keep-versions 2]
    python -m pipeline.run --skip-ingest      # rebuild silver and gold only, no downloads
"""

import argparse
import sys
import uuid

from pipeline import DB_PATH
from pipeline.checks import CheckFailed
from pipeline.ckan import DATASET_ID, extract_year_sources, fetch_resources
from pipeline.gold import build_gold
from pipeline.ingest import WarehouseLocked, cleanup_crashed_run, connect, ingest_year
from pipeline.silver import PREVIOUS, build_silver, drop_stage, restore_previous


def build_silver_and_gold(con, run_id):
    """Rebuild silver, then gold from it. If gold fails, silver is switched back to the tables
    it replaced, so the live silver and gold always come from the same build."""
    build_silver(con, run_id, keep_previous=True)
    try:
        build_gold(con, run_id)
    except Exception:
        restore_previous(con, run_id)
        raise
    drop_stage(con, PREVIOUS)


def ingest(con, args, run_id):
    resources = extract_year_sources(fetch_resources(args.dataset))
    if args.years:
        resources = [r for r in resources if r["year"] in args.years]

    print(f"\nrun {run_id}: ingesting {len(resources)} years into {args.db}")
    failed = []
    for resource in resources:
        try:
            result = ingest_year(con, resource, run_id, args.keep_versions)
            detail = (f"rows_with_new_id={result.rows_with_new_id} ids_not_in_old={result.ids_not_in_old}"
                      if result.rows_with_new_id is not None else "")
            if result.rows_added is not None:
                detail = f"+{result.rows_added} / -{result.rows_removed} rows"
            print(f"  {resource['year']}  v{result.version}  {result.change_type:<26} {detail}")
        except Exception as exc:
            failed.append(resource["year"])
            print(f"  {resource['year']}  FAILED: {exc}")
    if failed:
        print(f"\n{len(failed)} year(s) failed: {failed}. Their previous versions stay active.")
    return failed


def main(argv=None):
    parser = argparse.ArgumentParser(description="Run the pipeline locally.")
    parser.add_argument("--dataset", default=DATASET_ID)
    parser.add_argument("--db", default=str(DB_PATH))
    parser.add_argument("--years", type=int, nargs="*", help="only ingest these years (default: all)")
    parser.add_argument("--keep-versions", type=int, default=1,
                        help="full bronze versions per year to keep (default: 2, minimum 1)")
    parser.add_argument("--skip-ingest", action="store_true",
                        help="don't download; rebuild silver and gold from the bronze already loaded")
    args = parser.parse_args(argv)
    if args.keep_versions < 1:
        parser.error("--keep-versions must be at least 1")

    try:
        con = connect(args.db)
    except WarehouseLocked as exc:
        print(f"STOPPED: {exc}")
        return 1
    run_id = uuid.uuid4().hex
    problems = 0
    try:
        cleanup_crashed_run(con)
        if not args.skip_ingest:
            problems += len(ingest(con, args, run_id))

        print(f"\nrun {run_id}: building silver and gold")
        try:
            build_silver_and_gold(con, run_id)
        except CheckFailed as exc:
            problems += 1
            print(f"  STOPPED: {exc}. The live silver and gold tables were not replaced.")
    finally:
        con.close()
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main())
