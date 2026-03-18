
import json
from pathlib import Path
from typing import DefaultDict

# file_path = Path(__file__).parent.parent / "topics/chart_tags_2.json"
file_path = Path(__file__).parent / "chart_tags_2.json"

templates_file_path = Path(__file__).parent / "templates.json"
templates_data = json.loads(templates_file_path.read_text(encoding="utf-8"))
templates = {}
# { "title": "OWID/Deaths due to alcohol use", "source": "deaths-due-to-alcohol-use" }

for x in templates_data:
    templates[x["source"]] = x["title"]

"""
[
    {
        "tagId": 110,
        "tag_name": "Research & Development",
        "chartId": 8896,
        "slug": "number-of-entries-in-biological-sequence-databases",
        "title": "Number of entries in biological sequence databases"
    },
    {
        "tagId": 1579,
        "tag_name": "Medicine & Biotechnology",
        "chartId": 8896,
        "slug": "number-of-entries-in-biological-sequence-databases",
        "title": "Number of entries in biological sequence databases"
    },
"""
file_data = json.loads(file_path.read_text(encoding="utf-8"))

data = DefaultDict(list)

for x in file_data:
    page_slug = x["slug"]
    page_name = templates.get(page_slug, f"OWID/{x['title']}")
    data[x["tag_name"]].append(page_name)


def get_list(tags_names, category) -> list[str]:

    list_titles = data.get(category, [])

    for x in tags_names:
        list_titles.extend(data.get(x, []))

    print(f"get_list: result {len(list_titles)}")

    return [x.strip() for x in list_titles]
