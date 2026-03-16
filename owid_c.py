"""
"""

import json
import requests
from pathlib import Path
from typing import Dict, List

# OWID Datasette API
DATASETTE_API = "https://datasette-public.owid.io/owid.json"

path_dir = Path(__file__).parent


def fetch_from_sql() -> List[Dict]:
    """
    Fetch all charts that have hasMapTab or tab=map
    Using direct SQL query with pagination (LIMIT/OFFSET)
    """
    print("Fetching all charts from OWID database...")

    # First, get the total count
    total_count = fetch_total_chart_count()  # 7202

    # Base SQL query - note we add LIMIT and OFFSET dynamically
    sql_template = """
        select tagId, name as tag_name, chartId, cc.slug, cc.title
        from chart_tags
        left join charts cc ON cc.id = chartId
        left join tags ta ON ta.id = tagId
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

    charts = fetch_from_sql()
    save_file_json = path_dir / "chart_tags_2.json"

    with open(save_file_json, "w", encoding="utf-8") as f:
        json.dump(charts, f, ensure_ascii=False, indent=4)


if __name__ == "__main__":
    main()
