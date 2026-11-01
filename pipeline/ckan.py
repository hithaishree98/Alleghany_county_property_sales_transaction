"""CKAN API client for WPRDC, plus source discovery.

Discovery lists every resource on the dataset, keeps the year-named ones, and writes
their metadata (row counts, data dictionary) to data/ for reference.

Usage:
    python -m pipeline.ckan [--dataset real-estate-sales]
"""

import argparse
import csv
import json
import re

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from pipeline import DATA_DIR

CKAN_API = "https://data.wprdc.org/api/3/action"
DATASET_ID = "real-estate-sales"
YEAR_RE = re.compile(r"\b(19|20)\d{2}\b")
CORE_FIELDS = [
    "id", "name", "format", "url", "created", "last_modified",
    "metadata_modified", "datastore_active", "size",
]


# Shared HTTP session. Retries on connection errors and 5xx (WPRDC returns transient 502s)
# come from urllib3's Retry with exponential backoff, not a hand-written loop.
SESSION = requests.Session()
SESSION.mount("https://", HTTPAdapter(max_retries=Retry(
    total=5, backoff_factor=1, status_forcelist=[500, 502, 503, 504], allowed_methods=["GET"])))


def http_get(url, **kwargs):
    resp = SESSION.get(url, **kwargs)
    resp.raise_for_status()
    return resp


def ckan_get(action, **params):
    body = http_get(f"{CKAN_API}/{action}", params=params, timeout=60).json()
    if not body.get("success"):
        raise RuntimeError(f"CKAN {action} failed: {body.get('error')}")
    return body["result"]


def resource_state(resource_id):
    """What CKAN currently reports for a resource: last modified time, row count, columns.
    Read before and after a download to detect the source changing mid-transfer."""
    resource = ckan_get("resource_show", id=resource_id)
    datastore = ckan_get("datastore_search", resource_id=resource_id, limit=0)
    return {
        "last_modified": resource.get("last_modified"),
        "total": datastore["total"],
        "fields": [f["id"] for f in datastore["fields"]],
    }


def fetch_resources(dataset_id):
    resources = ckan_get("package_show", id=dataset_id)["resources"]
    print(f"All data sources in '{dataset_id}' ({len(resources)}):")
    for r in resources:
        print(f"  - {r['name']} [{r.get('format')}] {r['url']}")
    return resources


def extract_year_sources(resources):
    year_sources = []
    for r in resources:
        match = YEAR_RE.search(r["name"])
        if match:
            year_sources.append({**r, "year": int(match.group())})
    return sorted(year_sources, key=lambda r: r["year"])


def fetch_resource_metadata(resource):
    meta = {"year": resource["year"]}
    meta.update({k: resource.get(k) for k in CORE_FIELDS})
    meta["row_count"] = None
    meta["fields"] = []

    if resource.get("datastore_active"):
        ds = ckan_get("datastore_search", resource_id=resource["id"], limit=0)
        meta["row_count"] = ds.get("total")
        for f in ds.get("fields", []):
            if f["id"] == "_id":
                continue
            info = f.get("info") or {}
            meta["fields"].append({
                "column": f["id"],
                "type": f.get("type"),
                "label": info.get("label"),
                "notes": (info.get("notes") or "").strip(),
            })
    meta["column_count"] = len(meta["fields"])
    return meta


def write_outputs(metadata, out_dir):
    out_dir.mkdir(parents=True, exist_ok=True)

    summary_cols = ["year"] + CORE_FIELDS + ["row_count", "column_count"]
    with open(out_dir / "year_data_sources.csv", "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=summary_cols, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(metadata)

    with open(out_dir / "year_data_sources_metadata.json", "w", encoding="utf-8") as f:
        json.dump(metadata, f, indent=2)

    with open(out_dir / "data_dictionary.csv", "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["year", "resource_id", "column", "type", "label", "notes"])
        for m in metadata:
            for fld in m["fields"]:
                writer.writerow([m["year"], m["id"], fld["column"], fld["type"],
                                 fld["label"], fld["notes"]])


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--dataset", default=DATASET_ID)
    args = parser.parse_args(argv)

    resources = fetch_resources(args.dataset)
    year_sources = extract_year_sources(resources)

    print(f"\nData sources with a year in the name ({len(year_sources)}):")
    metadata = [fetch_resource_metadata(r) for r in year_sources]

    print(f"  {'Year':<6}{'Name':<40}{'Rows':>10}{'Cols':>6}  Last modified")
    for m in metadata:
        rows = f"{m['row_count']:,}" if m["row_count"] is not None else "-"
        print(f"  {m['year']:<6}{m['name']:<40}{rows:>10}{m['column_count']:>6}  {m['last_modified']}")

    write_outputs(metadata, DATA_DIR)
    print(f"\nWrote year_data_sources.csv, year_data_sources_metadata.json, data_dictionary.csv to {DATA_DIR}")


if __name__ == "__main__":
    main()
