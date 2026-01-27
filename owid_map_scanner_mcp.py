"""
OWID Grapher Map Scanner (MCP Version)
مسح جميع صفحات Grapher باستخدام OWID MCP

المخرجات: CSV file مع جميع الرسوم البيانية التي تدعم tab=map
"""

import csv
import json
import requests
from typing import Dict, List, Optional, Set

# OWID Datasette API
DATASETTE_API = "https://datasette-public.owid.io/owid.json"
GRAPHER_BASE_URL = "https://ourworldindata.org/grapher"


def fetch_map_charts_from_sql() -> List[Dict]:
    """
    جلب جميع الرسوم البيانية التي تحتوي على hasMapTab أو tab=map
    باستخدام استعلام SQL مباشر
    """
    print("جلب جميع الرسوم البيانية من قاعدة بيانات OWID...")

    # استعلام محسّن للعثور على جميع الرسوم البيانية التي تحتوي على خريطة
    # ملاحظة: JSON في Datasette يُخزن مع quotes مزدوجة (مثال: ""key"")
    sql = """
    SELECT id, slug, title, type, isPublished, config
    FROM charts
    WHERE config LIKE '%hasMapTab%'
       OR config LIKE '%"tab": "map"%'
       OR config LIKE '%"tab":"map"%'
    ORDER BY id
    """

    params = {
        "sql": sql,
        "_size": "50"  # اختبار بـ 50 نتيجة فقط
    }

    try:
        response = requests.get(DATASETTE_API, params=params, timeout=120)
        response.raise_for_status()

        # Datasette returns JSON, not CSV
        data = response.json()

        if not data.get("rows"):
            print("No results found")
            return []

        # Convert rows to list of dicts
        columns = data.get("columns", [])
        rows = data.get("rows", [])

        charts = []
        for row in rows:
            chart = dict(zip(columns, row))
            charts.append(chart)

        print(f"Found {len(charts)} charts with potential map support")
        return charts

    except Exception as e:
        print(f"Error fetching data: {e}")
        return []


def parse_config_for_map_info(config_str: str) -> Dict:
    """
    تحليل config JSON لاستخراج معلومات الخريطة
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
        # تنظيف JSON (إزالة التكرارات المزدوجة)
        config_str = config_str.replace('""', '"')
        config = json.loads(config_str)

        # التحقق من hasMapTab
        if config.get("hasMapTab"):
            info["has_map_tab"] = True

        # التحقق من tab الافتراضي
        if config.get("tab") == "map":
            info["default_tab"] = "map"
            info["has_map_tab"] = True

        # معلومات الخريطة
        if "map" in config:
            map_config = config["map"]
            info["map_column_slug"] = map_config.get("columnSlug")
            info["map_time"] = map_config.get("time")

            # التحقق من hideTimeline
            if map_config.get("hideTimeline"):
                info["has_timeline"] = False

        # نوع الكيان
        info["entity_type"] = config.get("entityType")

    except Exception:
        pass

    return info


def fetch_chart_data_years(slug: str) -> Set[int]:
    """
    جلب البيانات من CSV لاستخراج السنوات المتاحة
    """
    url = f"{GRAPHER_BASE_URL}/{slug}.csv"

    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()

        lines = response.text.strip().split("\n")
        if len(lines) < 2:
            return set()

        headers = lines[0].split(",")

        # البحث عن عمود السنة
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
        print(f"خطأ في جلب البيانات لـ {slug}: {e}")
        return set()


def check_single_year_map(slug: str, map_info: Dict) -> Optional[bool]:
    """
    التحقق مما إذا كانت الخريطة تحتوي على بيانات سنة واحدة فقط
    """
    # إذا كانت hideTimeline = true، فهي سنة واحدة
    if not map_info.get("has_timeline", True):
        return True

    # إذا كان map_time محدد، فهي سنة واحدة
    if map_info.get("map_time"):
        return True

    # Otherwise, fetch data to check
    years = fetch_chart_data_years(slug)
    if len(years) == 1:
        return True
    elif len(years) > 1:
        return False

    return None  # Unknown


def scan_all_charts(output_file: str = "owid_grapher_maps_complete.csv") -> List[Dict]:
    """
    مسح جميع الرسوم البيانية وإنشاء قائمة كاملة
    """
    print("=" * 60)
    print("OWID Grapher Map Scanner - Full Scan")
    print("=" * 60)
    print()

    # جلب جميع الرسوم البيانية
    charts = fetch_map_charts_from_sql()

    if not charts:
        print("لم يتم العثور على رسوم بيانية")
        return []

    results = []

    print("\nتحليل الرسوم البيانية...")
    print("-" * 60)

    for chart in charts:
        chart_id = chart.get("id", "")
        slug = chart.get("slug", "")
        title = chart.get("title", "")
        is_published = chart.get("isPublished", "")
        config_str = chart.get("config", "")

        # تحليل config
        map_info = parse_config_for_map_info(config_str)

        # إنشاء URL
        base_url = f"{GRAPHER_BASE_URL}/{slug}"
        map_url = f"{base_url}?tab=map" if map_info["has_map_tab"] else base_url

        # التحقق من السنة الواحدة
        single_year = check_single_year_map(slug, map_info)

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
    حفظ النتائج إلى CSV
    """
    print("\n" + "=" * 60)
    print(f"حفظ النتائج إلى {output_file}...")
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

    # إحصائيات
    map_charts = [r for r in results if r["has_map_tab"] == "Yes"]
    published = [r for r in results if r["is_published"] == "True"]
    single_year = [r for r in results if r["single_year_data"] == "Yes"]

    print()
    print("=== الإحصائيات ===")
    print(f"إجمالي الرسوم البيانية الممسوحة: {len(results)}")
    print(f"الرسوم البيانية مع خريطة: {len(map_charts)}")
    print(f"الرسوم البيانية المنشورة: {len(published)}")
    print(f"الخرائط ببيانات سنة واحدة: {len(single_year)}")
    print()

    # إنشاء ملف منفصل للخرائط المنشورة فقط
    published_map_charts = [r for r in map_charts if r["is_published"] == "True"]
    if published_map_charts:
        published_file = output_file.replace(".csv", "_published_only.csv")
        with open(published_file, "w", newline="", encoding="utf-8-sig") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(published_map_charts)
        print(f"تم حفظ {len(published_map_charts)} خريطة منشورة إلى: {published_file}")


def main():
    """
    الدالة الرئيسية
    """
    import sys
    import io

    # إصلاح ترميز الإخراج للنوافذ
    if sys.platform == "win32":
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

    print()
    print("=" * 60)
    print("OWID Grapher Map Scanner - MCP Version")
    print("Scan all Grapher pages for map support")
    print("=" * 60)
    print()

    results = scan_all_charts()

    if results:
        save_results(results, "owid_grapher_maps_complete.csv")

        print()
        print("✓ تم!")
        print()
        print("الملفات المُنشأة:")
        print("  1. owid_grapher_maps_complete.csv - جميع النتائج")
        print("  2. owid_grapher_maps_complete_published_only.csv - المنشورة فقط")


if __name__ == "__main__":
    main()
