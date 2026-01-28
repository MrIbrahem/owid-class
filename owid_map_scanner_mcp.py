"""
OWID Grapher Map Scanner (MCP Version)
Scan all Grapher pages for map tab support

Output: CSV file with all charts that support tab=map
https://datasette-public.owid.io/owid/charts
https://api.ourworldindata.org/v1/indicators/930012.metadata.json
https://api.ourworldindata.org/v1/indicators/930012.data.json
https://colab.research.google.com/drive/1f84soyHqXfcjcsJ-rxM2-SLdOn5ym3Oe#scrollTo=nFJ7jbHQKpSr
"""

import functools
import csv
import json
import requests
from tqdm import tqdm
from pathlib import Path
from typing import Dict, List, Optional, Set
from multiprocessing import Pool

# OWID Datasette API
DATASETTE_API = "https://datasette-public.owid.io/owid.json"
GRAPHER_BASE_URL = "https://ourworldindata.org/grapher"

try:
    path_dir = Path(__file__).parent
except NameError:
    path_dir = Path("/content/")

path_dir_csv_data = path_dir / "csv_data"
path_dir_csv_data.mkdir(parents=True, exist_ok=True)


def split_date(y):
    if isinstance(y, int):
        return y
    return y.split("-")[0]


def try_with_dimensions(dimensions: Optional[List[Dict]]) :
    """
    Try to determine single year map from dimensions
    """
    _example = [{"property": "y", "variableId": 923410}]
    years = set()
    if not dimensions:
        return years
    dimensions_dict = {dim.get("property"): dim.get("variableId") for dim in dimensions if "variableId" in dim}
    variableId = dimensions_dict.get("y")
    if not variableId:
        return years

    # load api
    url = f"https://api.ourworldindata.org/v1/indicators/{variableId}.metadata.json"
    data = {}
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        data = response.json()
    except Exception:
        return years
    # {"dimensions": { "years": { "values": [ { "id": 1980 }, { "id": 1981 }, ...}
    years_info = data.get("dimensions", {}).get("years", {}).get("values", [])
    years = [split_date(y.get("id")) for y in years_info if "id" in y]
    years = list(set(years))
    return years


def fetch_map_charts_from_sql() -> List[Dict]:
    """
    Fetch all charts that have hasMapTab or tab=map
    Using direct SQL query with pagination (LIMIT/OFFSET)
    """
    print("Fetching all charts from OWID database...")

    # First, get the total count
    total_count = fetch_total_chart_count()

    # Base SQL query - note we add LIMIT and OFFSET dynamically
    sql_template_0 = """
        SELECT id, slug, title, type, isPublished, config
        FROM charts
        WHERE config LIKE '%hasMapTab%'
        OR config LIKE '%"tab": "map"%'
        OR config LIKE '%"tab":"map"%'
        ORDER BY id
        LIMIT {limit} OFFSET {offset}
    """
    sql_template = """
        SELECT id, slug, title, type, isPublished, config
        FROM charts
        ORDER BY id
        LIMIT {limit} OFFSET {offset}
    """
    all_charts = []
    offset = 0
    page_size = 1000  # Max results per page in Datasette

    while True:
        sql = sql_template.format(limit=page_size, offset=offset)

        params = {
            "sql": sql,
            "_size": str(page_size)
        }

        try:
            response = requests.get(DATASETTE_API, params=params, timeout=120)
            response.raise_for_status()
            data = response.json()

            rows = data.get("rows", [])
            if not rows:
                # No more results
                break

            # Get columns from first page
            if offset == 0:
                columns = data.get("columns", [])

            # Convert rows to dicts
            for row in rows:
                chart = dict(zip(columns, row))
                all_charts.append(chart)

            # Progress message
            if total_count:
                progress = (len(all_charts) / total_count) * 100
                print(f"Fetched {len(rows)} charts (offset: {offset}, total: {len(all_charts)}/{total_count} - {progress:.1f}%)")
            else:
                print(f"Fetched {len(rows)} charts (offset: {offset}, total so far: {len(all_charts)})")

            # If we got less than page_size, we're done
            if len(rows) < page_size:
                break

            offset += page_size

        except Exception as e:
            print(f"Error fetching data at offset {offset}: {e}")
            break

    all_charts_by_slug = {
        chart["slug"]: {**chart, "config": parse_chart_config(chart["config"])} for chart in all_charts
    }
    # save first page data to file for debugging
    with open(path_dir / "all_charts.json", "w", encoding="utf-8") as f:
        json.dump(all_charts_by_slug, f, ensure_ascii=False, indent=4)

    print(f"Found {len(all_charts)} charts with potential map support")
    return all_charts


def parse_chart_config(config_str):
    try:
        return json.loads(config_str.replace('""', '"'))
    except Exception:
        return json.loads(config_str)


def fetch_total_chart_count():
    count_sql = """
    SELECT count(id) as total
    FROM charts
    WHERE config LIKE '%hasMapTab%'
       OR config LIKE '%"tab": "map"%'
       OR config LIKE '%"tab":"map"%'
    """

    count_sql = """
        SELECT count(id) as total
        FROM charts
        WHERE isPublished = 'true'
    """

    try:
        response = requests.get(DATASETTE_API, params={"sql": count_sql}, timeout=30)
        response.raise_for_status()
        data = response.json()
        total_count = data.get("rows", [[0]])[0][0]
        print(f"Total charts to fetch: {total_count}")
        print("-" * 50)
    except Exception as e:
        print(f"Warning: Could not get count: {e}")
        total_count = None
    return total_count


def parse_config_for_map_info(config: dict) -> Dict:
    """
    Parse config JSON to extract map information
    """
    info = {
        "has_map_tab": False,
        "default_tab": None,
        "map_column_slug": None,
        "map_time": None,
        "has_timeline": True,
        "entity_type": None,
        "max_time": None,
        "min_time": None,
    }

    timelineMaxTime = config.get("timelineMaxTime") or config.get("MaxTime")
    timelineMinTime = config.get("timelineMinTime") or config.get("MinTime")

    info["max_time"] = timelineMaxTime
    info["min_time"] = timelineMinTime

    # Check for hasMapTab
    if config.get("hasMapTab"):
        info["has_map_tab"] = True

    # Check for default tab
    if config.get("tab") == "map":
        info["default_tab"] = "map"
        info["has_map_tab"] = True

    # Map information
    if "map" in config:
        map_config = config["map"]
        info["map_column_slug"] = map_config.get("columnSlug")
        info["map_time"] = map_config.get("time")

        # Check for hideTimeline
        if map_config.get("hideTimeline"):
            info["has_timeline"] = False

    # Entity type
    info["entity_type"] = config.get("entityType")

    return info


@functools.lru_cache(maxsize=None)
def fetch_chart_data_years(slug: str) -> Set[int]:
    """
    Fetch data from CSV to extract available years
    """
    if not slug:
        return set()

    response_text = fetch_csv_data(slug)

    lines = response_text.strip().split("\n")
    if len(lines) < 2:
        return set()

    headers = lines[0].split(",")

    # Find year column
    year_col_idx = None
    for i, h in enumerate(headers):
        if h.strip().lower() in ["year", "time", "date"]:
            year_col_idx = i
            break

    if year_col_idx is None:
        # return set()
        year_col_idx = 2

    years = set()
    for line in lines[1:]:
        values = line.split(",")
        if len(values) > year_col_idx:
            try:
                year_str = split_date(values[year_col_idx].strip())
                year = int(float(year_str))
                years.add(year)
            except (ValueError, IndexError):
                continue

    return years


def fetch_csv_data(slug) -> str:
    csv_file_path = path_dir_csv_data / f"{slug}.csv"
    if csv_file_path.exists():
        with open(csv_file_path, "r", encoding="utf-8") as f:
            return f.read()
    text = ""

    try:
        url = f"{GRAPHER_BASE_URL}/{slug}.csv"
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        text = response.text
    except Exception as e:
        print(f"Error fetching CSV for {slug}: {e}")

    with open(csv_file_path, "w", encoding="utf-8") as f:
        f.write(text)

    return text


def check_single_year_map(slug: str, map_info: Dict):
    """
    Check if map has single year data only
    """
    # If hideTimeline = true, it's single year
    if not map_info.get("has_timeline", True):
        return True, "-"

    # If map_time is specified, it's single year
    if map_info.get("map_time"):
        return True, "-"

    timelineMaxTime = map_info.get("timelineMaxTime") or map_info.get("MaxTime")
    timelineMinTime = map_info.get("timelineMinTime") or map_info.get("MinTime")
    if timelineMaxTime is not None and timelineMinTime is not None:
        if timelineMaxTime == timelineMinTime:
            return True, "-"

    # return None

    # Otherwise, fetch data to check
    years = fetch_chart_data_years(slug)
    if len(years) == 1:
        return True, 1
    elif len(years) > 1:
        return False, len(years)

    return None, 0


def scan_all_charts_with_pool() -> List[Dict]:
    """
    Scan all charts and create complete list
    """
    print("=" * 60)
    print("OWID Grapher Map Scanner - Full Scan")
    print("=" * 60)
    print()

    # Fetch all charts
    charts = fetch_map_charts_from_sql()

    if not charts:
        print("No charts found")
        return []

    results = []

    print("\nAnalyzing charts...")
    print("-" * 60)

    with Pool(processes=4) as pool:
        results = list(pool.imap(generate_chart_result, charts))

    return results


def generate_chart_result(chart):
    chart_id = chart.get("id", "")
    slug = chart.get("slug", "")
    title = chart.get("title", "")
    is_published = chart.get("isPublished", "")
    config_str = chart.get("config", "")

    # Clean JSON (remove double quotes)
    config = parse_chart_config(config_str)

    # Parse config
    map_info = parse_config_for_map_info(config)

    # Create URL
    base_url = f"{GRAPHER_BASE_URL}/{slug}"
    map_url = f"{base_url}?tab=map" if map_info["has_map_tab"] else base_url

    years = fetch_chart_data_years(slug)

    if not years:
        years = try_with_dimensions(config.get("dimensions"))

    len_years = len(years)

    max_time = map_info.get("max_time") or (max(years) if years else None)
    min_time = map_info.get("min_time") or (min(years) if years else None)

    result = {
        "chart_id": chart_id,
        "slug": slug,
        "title": title,
        "url": map_url,
        "has_map_tab": "Yes" if map_info["has_map_tab"] else "No",
        "max_time": max_time,
        "min_time": min_time,
        "default_tab": map_info.get("default_tab", ""),
        "is_published": is_published,
        "entity_type": map_info.get("entity_type", ""),
        "single_year_data": "Yes" if len(years) == 1 else ("No" if len(years) > 1 else "Unknown"),
        "len_years": len_years,
        "has_timeline": "Yes" if map_info.get("has_timeline") else "No"
    }

    return result


def save_results(results: List[Dict], output_file: str):
    """
    Save results to CSV
    """
    print("\n" + "=" * 60)
    print(f"Saving results to {output_file}...")
    print("=" * 60)

    fieldnames = [
        "chart_id", "slug", "title", "url",
        "has_map_tab", "max_time", "min_time", "default_tab", "is_published",
        "entity_type", "single_year_data", "len_years", "has_timeline"
    ]

    with open(output_file, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(results)

    # Statistics
    map_charts = [r for r in results if r["has_map_tab"] == "Yes"]
    published = [r for r in results if (r["is_published"] == "True" or r["is_published"] is True)]
    single_year = [r for r in results if r["single_year_data"] == "Yes"]
    year_status_unknown = [r for r in results if r["single_year_data"] == "Unknown"]

    print()
    print("=== Statistics ===")
    print(f"Total charts scanned: {len(results)}")
    print(f"Charts with map: {len(map_charts)}")
    print(f"Published charts: {len(published)}")
    print(f"Single year maps: {len(single_year)}")
    print(f"Maps with unknown year status: {len(year_status_unknown)}")
    print()

    # Create separate file for published maps only
    published_map_charts = [r for r in map_charts if (r["is_published"] == "True" or r["is_published"] is True)]
    if published_map_charts:
        published_file = output_file.with_name(output_file.stem + "_published_only.csv")
        with open(published_file, "w", newline="", encoding="utf-8-sig") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(published_map_charts)
        print(f"Saved {len(published_map_charts)} published maps to: {published_file}")


def main():
    """
    Main function
    """

    # Fix output encoding for Windows
    # if sys.platform == "win32": sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

    print()
    print("=" * 60)
    print("OWID Grapher Map Scanner - MCP Version")
    print("Scan all Grapher pages for map support")
    print("=" * 60)
    print()

    results = scan_all_charts_with_pool()
    save_file_json = path_dir / "owid_grapher_maps_complete.json"

    with open(save_file_json, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=4)

    save_file = path_dir / "owid_grapher_maps_complete.csv"
    if results:
        save_results(results, save_file)

        print()
        print("Done!")
        print()
        print("Files created:")
        print(f"  1. {save_file} - All results")
        print(f"  2. {save_file.with_name(save_file.stem + '_published_only.csv')} - Published only")


if __name__ == "__main__":
    print(try_with_dimensions([{"property": "y", "variableId": 922894}]))
    print(fetch_chart_data_years("weekly-hospital-admissions-covid-per-million"))
    main()
