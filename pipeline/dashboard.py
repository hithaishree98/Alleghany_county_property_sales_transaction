"""Gold -> dashboard: one self-contained HTML page for people who use the County's data.

Laid out like the dashboards on Allegheny Analytics (analytics.alleghenycounty.us): a Home tab,
Data and Definitions, a County Summary, and a Municipality tab with a map and a profile that
compares up to six places. Every number comes from the gold table through the queries in
pipeline/insights.py, the same ones notebook 04 and the README quote.

    build_dashboard   writes dashboard/home_sale_prices.html and the gold table as CSV
    area_shapes       municipality and ward outlines for the map, downloaded once

The page is rebuilt at the end of every pipeline run (pipeline/run.py), so it refreshes when
the data does. If the outlines can't be downloaded, the page is written without the map.

Usage (from the project root):
    python -m pipeline.dashboard        # rebuild from the live gold table
"""

import json
import os
import sys
from datetime import datetime

import duckdb
import requests

from pipeline import DATA_DIR, DB_PATH, PROJECT_ROOT
from pipeline import insights as Q
from pipeline.silver import LIVE as SILVER

DASHBOARD_DIR = PROJECT_ROOT / "dashboard"
PAGE_NAME = "home_sale_prices.html"
CSV_NAME = "valid_sale_prices_by_municipality_year.csv"
TEMPLATE = PROJECT_ROOT / "pipeline" / "dashboard_template.html"

# Allegheny County Voting District Boundaries (WPRDC), in PA State Plane feet (EPSG:2272). Its
# OPA_MUNI_1 is the assessment municipality code the sales use; Pittsburgh, Clairton, Duquesne
# and McKeesport are 100/200/300/400 there and split into wards in the sales (code + ward).
DISTRICTS_URL = ("https://data.wprdc.org/dataset/df953315-6b4d-47e6-9502-d5a6af83d993/resource/"
                 "c7dba60a-3631-4a04-8221-951067397062/download/data.geojson")
SHAPES = DATA_DIR / "boundaries" / "sale_areas.json"
SIMPLIFY_FEET = 120          # outline detail kept; finer only makes the page bigger
MAP_WIDTH = 1000             # SVG viewBox width the outlines are scaled to
MIN_MAP_MATCH = 0.9          # share of gold codes that need an outline for the map to be shown


def rows(con, sql):
    cur = con.execute(sql)
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, r)) for r in cur.fetchall()]


def area_shapes():
    """{muni_code: SVG path} for every municipality and ward, plus the viewBox height. Built
    from the district outlines on the first call and kept in data/boundaries/."""
    if SHAPES.exists():
        return json.loads(SHAPES.read_text(encoding="utf-8"))
    SHAPES.parent.mkdir(parents=True, exist_ok=True)
    raw = SHAPES.with_name("voting_districts.geojson")
    if not raw.exists():
        response = requests.get(DISTRICTS_URL, timeout=300)
        response.raise_for_status()
        raw.write_bytes(response.content)

    con = duckdb.connect()
    try:
        con.execute("INSTALL spatial")
        con.execute("LOAD spatial")
        areas = con.execute(f"""
            WITH d AS (
                SELECT CASE WHEN OPA_MUNI_1 IN (100, 200, 300, 400) THEN OPA_MUNI_1 + WARD_1
                            ELSE OPA_MUNI_1 END::VARCHAR AS muni_code, geom
                FROM ST_Read('{raw.as_posix()}')
            )
            SELECT muni_code, ST_AsGeoJSON(ST_SimplifyPreserveTopology(ST_Union_Agg(geom), {SIMPLIFY_FEET}))
            FROM d GROUP BY 1
        """).fetchall()
        minx, miny, maxx, maxy = con.execute(f"""
            SELECT min(ST_XMin(geom)), min(ST_YMin(geom)), max(ST_XMax(geom)), max(ST_YMax(geom))
            FROM ST_Read('{raw.as_posix()}')
        """).fetchall()[0]
    finally:
        con.close()

    scale = MAP_WIDTH / (maxx - minx)

    def ring(points):
        return "M" + "L".join(f"{(x - minx) * scale:.1f},{(maxy - y) * scale:.1f}" for x, y in points) + "Z"

    paths = {}
    for code, geojson in areas:
        g = json.loads(geojson)
        polygons = g["coordinates"] if g["type"] == "MultiPolygon" else [g["coordinates"]]
        paths[code] = "".join(ring(r) for polygon in polygons for r in polygon)
    shapes = {"height": round((maxy - miny) * scale, 1), "paths": paths}
    SHAPES.write_text(json.dumps(shapes), encoding="utf-8")
    return shapes


def page_data(con, shapes):
    """Everything the page draws, as plain JSON-able values."""
    latest = Q.latest_complete_year(con)
    jump = Q.jump_year(con)
    years = [y for (y,) in con.execute(f"SELECT DISTINCT sale_year FROM {Q.G} ORDER BY 1").fetchall()]
    names = dict(con.execute(
        f"SELECT muni_code, any_value(trim(regexp_replace(muni_desc, '\\s+', ' ', 'g'))) FROM {Q.G} GROUP BY 1"
    ).fetchall())
    gold = con.execute(f"""
        SELECT sale_year, muni_code, median_price, p25_price, p75_price, valid_sales_used,
               is_low_sample, is_provisional
        FROM {Q.G} ORDER BY muni_code, sale_year
    """).fetchall()
    data_through = con.execute(f"SELECT max(sale_date) FROM {SILVER['records']}").fetchall()[0][0]

    comparisons = {}
    if latest is not None:
        comparisons["quarters"] = rows(con, Q.quarter_growth(latest))
        comparisons["ends"] = {y: rows(con, Q.highest_lowest(y)) for y in years}
    if latest is not None and jump is not None:
        before_after = rows(con, Q.before_after(jump, latest))
        comparisons["before_after"] = before_after[0] if before_after and before_after[0]["municipalities"] else None

    now = datetime.now()
    return {
        "built_at": f"{now:%B} {now.day}, {now:%Y %H:%M}",
        "data_through": str(data_through) if data_through else None,
        "latest": latest, "jump": jump, "years": years,
        "default_muni": Q.most_sales_municipality(con),
        "min_sales": Q.MIN_SALES, "min_municipalities": Q.MIN_MUNICIPALITIES,
        "names": names, "gold": [list(r) for r in gold],
        "overview": rows(con, Q.overview())[0],
        "coverage": rows(con, Q.coverage_by_year()),
        "change": rows(con, Q.typical_change()),
        "spread": rows(con, Q.spread_and_rank()),
        **comparisons,
        "csv": CSV_NAME,
        "map": shapes,
    }


def map_shapes_for(con):
    """The outlines, or None (with the reason printed) when they're missing or don't match."""
    try:
        shapes = area_shapes()
    except Exception as exc:
        print(f"  dashboard: no map ({exc.__class__.__name__}: {str(exc)[:200]})")
        return None
    codes = [c for (c,) in con.execute(f"SELECT DISTINCT muni_code FROM {Q.G}").fetchall()]
    missing = sorted(c for c in codes if c not in shapes["paths"])
    matched = len(codes) - len(missing)
    print(f"  dashboard: map outlines for {matched}/{len(codes)} municipality codes"
          + (f" (missing: {', '.join(missing[:20])})" if missing else ""))
    if not codes or matched / len(codes) < MIN_MAP_MATCH:
        print("  dashboard: too few outlines match; the map is left out")
        return None
    return {"height": shapes["height"], "paths": {c: shapes["paths"][c] for c in codes if c in shapes["paths"]}}


def build_dashboard(con, out_dir=DASHBOARD_DIR, with_map=True):
    """Write the page and the CSV into out_dir. Returns the page path."""
    out_dir.mkdir(parents=True, exist_ok=True)
    shapes = map_shapes_for(con) if with_map else None
    data = json.dumps(page_data(con, shapes), default=str, separators=(",", ":")).replace("</", "<\\/")
    html = TEMPLATE.read_text(encoding="utf-8").replace("/*__DATA__*/null", data)

    # Written next to the target and swapped in, so a reader never sees half a page
    page, csv = out_dir / PAGE_NAME, out_dir / CSV_NAME
    tmp_page, tmp_csv = page.with_suffix(".html.tmp"), csv.with_suffix(".csv.tmp")
    tmp_page.write_text(html, encoding="utf-8")
    con.execute(f"COPY (SELECT * FROM {Q.G} ORDER BY sale_year, muni_code) "
                f"TO '{tmp_csv.as_posix().replace(chr(39), chr(39) * 2)}' (HEADER, DELIMITER ',')")
    os.replace(tmp_page, page)
    os.replace(tmp_csv, csv)
    print(f"  dashboard: wrote {page.relative_to(PROJECT_ROOT) if page.is_relative_to(PROJECT_ROOT) else page}")
    return page


def main():
    try:
        con = duckdb.connect(str(DB_PATH), read_only=True)
    except duckdb.IOException as exc:
        print(f"STOPPED: the warehouse is in use ({exc})")
        return 1
    try:
        build_dashboard(con)
    finally:
        con.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
