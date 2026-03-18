
import json
import sys
from tqdm import tqdm
from pathlib import Path

from chart_tags_2 import get_list
from api_client import add_category_to_pages, create_category, get_global_saves

not_exists_pages = []
exists_pages = []

main_dir = Path(__file__).parent
file_path = main_dir / "topicTagGraph_2.json"
ids_slug_path = main_dir / "topicTagGraph_id.json"

data_list = json.loads(file_path.read_text(encoding="utf-8"))

ids_slug_data = json.loads(ids_slug_path.read_text(encoding="utf-8"))

to_save = {}


def create_category_text(main_categories, category_name, sub_categories) -> str:
    text = []

    main_category_text = create_main_category_text(main_categories)

    category_title = category_name.replace("Category:Our World in Data - ", "")
    text.append(f"[[:Category:Our World in Data topics|Our World in Data topics]] > {main_category_text} > [[:{category_name}|{category_title}]]:")

    if sub_categories:
        text.append("\n" + "Topics in this category:")

        text_cats = create_sub_categories_text(sub_categories)
        text.extend(text_cats)

    for main_category in main_categories:
        text.append(f"[[Category:Our World in Data - {main_category}| ]]")

    return "\n".join(text)


def create_sub_categories_text(sub_categories) -> list:
    text_cats = []

    for x in sub_categories:
        x_text = slug_link(x)
        x_text_formated = f"* {x_text}" if len(sub_categories) < 5 else x_text
        text_cats.append(x_text_formated)

    if len(sub_categories) < 5:
        return text_cats

    text_cats_text = ", ".join(text_cats)

    return [f"* {text_cats_text}"]


def create_main_category_text(main_categories) -> str:
    list_cats = []

    for main_category in main_categories:
        list_cats.append(f"[[:Category:Our World in Data - {main_category}|{main_category}]]")

    if len(list_cats) == 1:
        return list_cats[0]

    main_category_text = "/".join(list_cats)

    return f"({main_category_text})"


def slug_link(x) -> str:
    x_text = ""
    if ids_slug_data.get(x, {}).get("slug"):
        x_text = f"[https://ourworldindata.org/{ids_slug_data[x]['slug']} {x}]"
    return x_text


to_create = {}

for x, v in tqdm(data_list.items()):
    for category, sub_categories in v.items():

        category_name = f"Category:Our World in Data - {category}"
        category_name = category_name.replace(" & ", " and ")

        cat_data = {
            "main": [x],
            "sub_categories": sub_categories,
            "category_name": category_name,
        }
        to_create.setdefault(category, cat_data)
        if x not in to_create[category]["main"]:
            to_create[category]["main"].append(x)

with open(main_dir / "to_create.json", "w", encoding="utf-8") as f:
    json.dump(to_create, f, indent=4, ensure_ascii=False)

for n, (category, v) in enumerate(to_create.items(), start=1):
    print(f"{n}/{len(to_create)}: {category=}")

    if n == 5 and "break" in sys.argv:
        break

    sub_categories = v["sub_categories"]
    main_categories = v["main"]
    category_name = v["category_name"]

    # if len(main_categories) == 1: continue

    text = create_category_text(main_categories, category_name, sub_categories)

    to_save[category_name] = text

    create_category(category_name, text)

    titles = get_list(list(sub_categories), category)

    add_category_to_pages(category_name, titles)
