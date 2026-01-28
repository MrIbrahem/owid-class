"""
OWID Grapher Map Scanner (MCP Version)
Scan all Grapher pages for map tab support

Output: CSV file with all charts that support tab=map
https://datasette-public.owid.io/owid/charts
"""

import csv
import json
import requests
from pathlib import Path
from typing import Dict, List, Optional, Set

# OWID Datasette API
DATASETTE_API = "https://datasette-public.owid.io/owid.json"
GRAPHER_BASE_URL = "https://ourworldindata.org/grapher"


def fetch_map_charts_from_sql() -> List[Dict]:
    """
    Fetch all charts that have hasMapTab or tab=map
    Using direct SQL query with pagination (LIMIT/OFFSET)
    """
    print("Fetching all charts from OWID database...")

    # First, get the total count
    count_sql = """
    SELECT count(id) as total
    FROM charts
    WHERE config LIKE '%hasMapTab%'
       OR config LIKE '%"tab": "map"%'
       OR config LIKE '%"tab":"map"%'
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

    # Base SQL query - note we add LIMIT and OFFSET dynamically
    sql_template = """
    SELECT id, slug, title, type, isPublished, config
    FROM charts
    WHERE config LIKE '%hasMapTab%'
       OR config LIKE '%"tab": "map"%'
       OR config LIKE '%"tab":"map"%'
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
                # save first page data to file for debugging
                with open(Path(__file__).parent / "debug_data.json", "w", encoding="utf-8") as f:
                    json.dump(data, f, ensure_ascii=False, indent=4)

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

    print(f"Found {len(all_charts)} charts with potential map support")
    return all_charts


def parse_config_for_map_info(config_str: str) -> Dict:
    """
    Parse config JSON to extract map information
    """
    info = {
        "has_map_tab": False,
        "default_tab": None,
        "map_column_slug": None,
        "map_time": None,
        "has_timeline": True,
        "entity_type": None
    }

    try:
        # Clean JSON (remove double quotes)
        config_str = config_str.replace('""', '"')
        config = json.loads(config_str)

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

    except Exception:
        pass

    return info


def fetch_chart_data_years(slug: str) -> Set[int]:
    """
    Fetch data from CSV to extract available years
    """
    url = f"{GRAPHER_BASE_URL}/{slug}.csv"

    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()

        lines = response.text.strip().split("\n")
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
            return set()

        years = set()
        for line in lines[1:]:
            values = line.split(",")
            if len(values) > year_col_idx:
                try:
                    year = int(float(values[year_col_idx]))
                    years.add(year)
                except (ValueError, IndexError):
                    continue

        return years

    except Exception as e:
        print(f"Error fetching data for {slug}: {e}")
        return set()


def check_single_year_map(slug: str, map_info: Dict) -> Optional[bool]:
    """
    Check if map has single year data only
    """
    # If hideTimeline = true, it's single year
    if not map_info.get("has_timeline", True):
        return True

    # If map_time is specified, it's single year
    if map_info.get("map_time"):
        return True

    # Otherwise, fetch data to check
    years = fetch_chart_data_years(slug)
    if len(years) == 1:
        return True
    elif len(years) > 1:
        return False

    return None  # Unknown


def scan_all_charts() -> List[Dict]:
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

    # Run 50 only for testing
    for chart in charts:  # [:50]:
        chart_id = chart.get("id", "")
        slug = chart.get("slug", "")
        title = chart.get("title", "")
        is_published = chart.get("isPublished", "")
        config_str = chart.get("config", "")

        # Parse config
        map_info = parse_config_for_map_info(config_str)

        # Create URL
        base_url = f"{GRAPHER_BASE_URL}/{slug}"
        map_url = f"{base_url}?tab=map" if map_info["has_map_tab"] else base_url

        # Check for single year
        # single_year = check_single_year_map(slug, map_info)
        single_year = None
        result = {
            "chart_id": chart_id,
            "slug": slug,
            "title": title,
            "url": map_url,
            "has_map_tab": "Yes" if map_info["has_map_tab"] else "No",
            "default_tab": map_info.get("default_tab", ""),
            "is_published": is_published,
            "entity_type": map_info.get("entity_type", ""),
            "single_year_data": "Yes" if single_year else ("No" if single_year is False else "Unknown"),
            "has_timeline": "Yes" if map_info.get("has_timeline") else "No"
        }

        results.append(result)

        status = "MAP" if map_info["has_map_tab"] else "no map"
        published = "PUB" if is_published == "True" else "draft"
        print(f"[{status}] [{published}] {slug}: {title[:50]}")

    return results


def save_results(results: List[Dict], output_file: str):
    """
    Save results to CSV
    """
    print("\n" + "=" * 60)
    print(f"Saving results to {output_file}...")
    print("=" * 60)

    fieldnames = [
        "chart_id", "slug", "title", "url",
        "has_map_tab", "default_tab", "is_published",
        "entity_type", "single_year_data", "has_timeline"
    ]

    with open(output_file, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(results)

    # Statistics
    map_charts = [r for r in results if r["has_map_tab"] == "Yes"]
    published = [r for r in results if r["is_published"] == "True"]
    single_year = [r for r in results if r["single_year_data"] == "Yes"]

    print()
    print("=== Statistics ===")
    print(f"Total charts scanned: {len(results)}")
    print(f"Charts with map: {len(map_charts)}")
    print(f"Published charts: {len(published)}")
    print(f"Single year maps: {len(single_year)}")
    print()

    # Create separate file for published maps only
    published_map_charts = [r for r in map_charts if r["is_published"] == "True"]
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

    results = scan_all_charts()
    save_file = Path(__file__).parent / "owid_grapher_maps_complete.csv"
    if results:
        save_results(results, save_file)

        print()
        print("Done!")
        print()
        print("Files created:")
        print(f"  1. {save_file} - All results")
        print(f"  2. {save_file.with_name(save_file.stem + '_published_only.csv')} - Published only")


if __name__ == "__main__":
    main()
