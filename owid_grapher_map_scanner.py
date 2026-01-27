"""
OWID Grapher Map Scanner
مسح جميع صفحات Grapher والكشف عن الصفحات التي تدعم tab=map

المخرجات: CSV file مع:
- URL
- Title
- Has Map Tab
- Is Published
- Single Year Data (flag)
"""

import csv
import requests
import json
from typing import Dict, List, Optional, Set
from concurrent.futures import ThreadPoolExecutor, as_completed
import time

# Datasette API endpoint
DATASETTE_API = "https://datasette-public.owid.io/owid.json"
GRAPHER_BASE_URL = "https://ourworldindata.org/grapher"

# قائمة الرسوم البيانية التي نعرف أنها تحتوي على hasMapTab
# من البيانات المستخرجة سابقاً
KNOWN_MAP_CHARTS = [
    ("390", "population-with-un-projections", "Population"),
    ("8903", "most-common-religion", "What is the most common religious affiliation in each country?"),
    ("8910", "trust-churches-religious-organizations", "Share of people that trust churches and religious organizations"),
    ("8911", "trust-another-religion", "Share of people who say they trust people of another religion"),
    ("8912", "neighbors-different-religion", "Share who said they would not want neighbors of a different religion"),
    ("703", "share-of-children-younger-than-5-who-suffer-from-stunting", "Malnutrition: Share of children who are stunted"),
]


def fetch_all_chart_ids() -> List[Dict]:
    """جلب جميع معرفات الرسوم البيانية من قاعدة البيانات"""
    print("جلب جميع الرسوم البيانية...")

    # استعلام SQL للحصول على جميع الرسوم البيانية
    sql = """
    SELECT id, slug, title, type, isPublished
    FROM charts
    WHERE config LIKE '%"hasMapTab":1%'
       OR config LIKE '%"hasMapTab":true%'
       OR config LIKE '%"tab":"map"%'
    ORDER BY id
    """

    params = {
        "sql": sql,
        "_size": "max"  # الحد الأقصى للنتائج
    }

    try:
        response = requests.get(DATASETTE_API, params=params, timeout=60)
        response.raise_for_status()
        data = response.json()

        # تحليل CSV من الاستجابة
        lines = data.get("csv", "").strip().split("\n")
        if len(lines) < 2:
            print("لم يتم العثور على رسوم بيانية")
            return []

        headers = lines[0].split(",")
        charts = []

        for line in lines[1:]:
            values = line.split(",")
            if len(values) >= 5:
                charts.append({
                    "id": values[0],
                    "slug": values[1],
                    "title": values[2],
                    "type": values[3],
                    "isPublished": values[4]
                })

        print(f"تم العثور على {len(charts)} رسم بياني محتمل يحتوي على خريطة")
        return charts

    except Exception as e:
        print(f"خطأ في جلب البيانات: {e}")
        return KNOWN_MAP_CHARTS  # استخدام القائمة المعروفة كمثال


def check_chart_for_map(chart_id: str, slug: str) -> Dict:
    """
    التحقق مما إذا كان الرسم البياني يدعم tab=map
    عن طريق جلب بيانات CSV والتحقق من البنية
    """
    url = f"{GRAPHER_BASE_URL}/{slug}.csv"
    result = {
        "id": chart_id,
        "slug": slug,
        "has_map": False,
        "single_year": False,
        "years": set(),
        "error": None
    }

    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()

        # تحليل CSV
        lines = response.text.strip().split("\n")
        if len(lines) < 2:
            result["error"] = "No data"
            return result

        headers = lines[0].split(",")

        # البحث عن عمود السنة
        year_col_idx = None
        for i, h in enumerate(headers):
            if h.lower() in ["year", "time", "date"]:
                year_col_idx = i
                break

        if year_col_idx is None:
            result["has_map"] = False
            result["error"] = "No year column found"
            return result

        # جمع السنوات الفريدة
        for line in lines[1:]:
            values = line.split(",")
            if len(values) > year_col_idx:
                try:
                    year = int(float(values[year_col_idx]))
                    result["years"].add(year)
                except (ValueError, IndexError):
                    continue

        # التحقق من عدد السنوات
        if len(result["years"]) == 1:
            result["single_year"] = True

        # افتراض أن الرسم البياني يحتوي على خريطة إذا كان لديه بيانات جغرافية
        # (هذا تبسيط - للتحقق الدقيق نحتاج لفحص JSON config)
        result["has_map"] = len(result["years"]) > 0

    except Exception as e:
        result["error"] = str(e)

    return result


def check_map_tab_via_html(slug: str) -> bool:
    """
    التحقق من وجود tab=map في صفحة HTML
    """
    url = f"{GRAPHER_BASE_URL}/{slug}"

    try:
        response = requests.get(url, timeout=30, headers={
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        })
        response.raise_for_status()

        html = response.text

        # التحقق من وجود علامات tab=map
        indicators = [
            '"tab":"map"',
            '"hasMapTab":true',
            '"hasMapTab":1',
            'data-tab="map"',
            'class="map-tab"',
            'tab=map'
        ]

        for indicator in indicators:
            if indicator in html:
                return True

        return False

    except Exception as e:
        print(f"خطأ في التحقق من {slug}: {e}")
        return False


def scan_grapher_pages(max_charts: Optional[int] = None) -> List[Dict]:
    """
    مسح جميع صفحات Grapher والكشف عن الخرائط
    """
    charts = fetch_all_chart_ids()

    if max_charts:
        charts = charts[:max_charts]

    results = []

    print(f"\nبدء مسح {len(charts)} رسم بياني...")

    # استخدام ThreadPoolExecutor للتوازي
    with ThreadPoolExecutor(max_workers=10) as executor:
        future_to_chart = {}

        for chart in charts:
            chart_id = chart["id"]
            slug = chart["slug"]
            title = chart.get("title", "")

            # التحقق من وجود tab=map عبر HTML
            future = executor.submit(check_map_tab_via_html, slug)
            future_to_chart[future] = (chart_id, slug, title, chart)

        for future in as_completed(future_to_chart):
            chart_id, slug, title, chart_info = future_to_chart[future]

            try:
                has_map = future.result()

                result = {
                    "chart_id": chart_id,
                    "slug": slug,
                    "title": title,
                    "url": f"{GRAPHER_BASE_URL}/{slug}?tab=map" if has_map else f"{GRAPHER_BASE_URL}/{slug}",
                    "has_map_tab": "Yes" if has_map else "No",
                    "is_published": chart_info.get("isPublished", "Unknown"),
                    "single_year_data": "To be checked"
                }

                results.append(result)
                print(f"✓ {slug}: {'Has Map' if has_map else 'No Map'}")

            except Exception as e:
                print(f"✗ {slug}: Error - {e}")

    return results


def save_to_csv(results: List[Dict], filename: str = "owid_grapher_maps.csv"):
    """حفظ النتائج إلى ملف CSV"""
    print(f"\nحفظ النتائج إلى {filename}...")

    with open(filename, "w", newline="", encoding="utf-8-sig") as f:
        fieldnames = ["chart_id", "slug", "title", "url", "has_map_tab", "is_published", "single_year_data"]
        writer = csv.DictWriter(f, fieldnames=fieldnames)

        writer.writeheader()
        writer.writerows(results)

    print(f"تم حفظ {len(results)} سطر إلى {filename}")

    # طباعة إحصائيات
    map_charts = [r for r in results if r["has_map_tab"] == "Yes"]
    published_charts = [r for r in results if r["is_published"] == "True"]

    print("\n=== الإحصائيات ===")
    print(f"إجمالي الرسوم البيانية الممسوحة: {len(results)}")
    print(f"الرسوم البيانية مع خريطة: {len(map_charts)}")
    print(f"الرسوم البيانية المنشورة: {len(published_charts)}")


def main():
    """الدالة الرئيسية"""
    print("=== OWID Grapher Map Scanner ===")
    print("مسح جميع صفحات Grapher للكشف عن الخرائط التفاعلية\n")

    # مسح الصفحات (استخدم max_charts=None لمسح الكل)
    results = scan_grapher_pages(max_charts=50)  # ابدأ بـ 50 للاختبار

    # حفظ النتائج
    save_to_csv(results)

    print("\nتم!")
    print(f"الملف: owid_grapher_maps.csv")


if __name__ == "__main__":
    main()
