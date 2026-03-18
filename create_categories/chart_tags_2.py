
import json
from pathlib import Path
from typing import DefaultDict

# file_path = Path(__file__).parent.parent / "topics/chart_tags_2.json"
file_path = Path(__file__).parent / "chart_tags_2.json"

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
    data[x["tag_name"]].append(f"OWID/{x['title']}")


def get_list(tags_names, category) -> list[str]:

    list_titles = data.get(category, [])

    for x in tags_names:
        list_titles.extend(data.get(x, []))

    print(f"get_list: result {len(list_titles)}")

    return list_titles
