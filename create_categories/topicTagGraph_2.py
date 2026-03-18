

import functools
# import sys
import os
import json
import mwclient
from tqdm import tqdm
from pathlib import Path
from dotenv import load_dotenv

from chart_tags_2 import get_list

load_dotenv()

main_dir = Path(__file__).parent
file_path = main_dir / "topicTagGraph_2.json"
ids_slug_path = main_dir / "topicTagGraph_id.json"

data_list = json.loads(file_path.read_text(encoding="utf-8"))

ids_slug_data = json.loads(ids_slug_path.read_text(encoding="utf-8"))

to_save = {}


@functools.lru_cache(maxsize=1)
def initialize_site_connection(username, password):
    site_mw = mwclient.Site('commons.wikimedia.org')
    print(f"loging in as {username}")

    site_mw.login(username, password)
    if site_mw.logged_in:
        print("Logged in successfully")
    else:
        print("Failed to log in")
    return site_mw


class page_mwclient:
    def __init__(self, title: str):
        self.title = title
        self.username = os.environ.get("WIKI_USERNAME")
        self.password = os.environ.get("WIKI_PASSWORD")

        self.site_mw = initialize_site_connection(self.username, self.password)

        self.page = self.site_mw.pages[title]

    def get_text(self):
        return self.page.text()

    def exists(self):
        return self.page.exists

    def save(self, newtext: str, summary: str):
        # if "ask" in sys.argv:
        ask = input(f"Do you want to save the changes? (y/n): {summary=}")
        yess = ["", "y", "a"]
        if ask not in yess:
            return False

        result = self.page.save(newtext, summary=summary)
        print(f"Saved page {self.title} with result: {result}")
        return result

    def create(self, newtext: str, summary: str):
        ask = input(f"Do you want to create the page? (y/n): {summary=}")
        yess = ["", "y", "a"]
        if ask not in yess:
            return False
        result = self.page.save(newtext, summary=summary)
        print(f"Created page {self.title} with result: {result}")
        return result


def create_category_text(main_categories, category_name, sub_categories) -> str:
    text = []

    main_category_text = creta_main_category_text(main_categories)

    category_title = category_name.replace("Category:Our World in Data - ", "")
    text.append(f"[[:Category:Our World in Data topics|Our World in Data topics]] > {main_category_text} > [[:{category_name}|{category_title}]]:")

    text.append("\nTopics in this category:")

    for x in sub_categories:
        x_text = slug_link(x)
        text.append(f"* {x_text}")

    for main_category in main_categories:
        text.append(f"[[Category:Our World in Data - {main_category}| ]]")

    return "\n".join(text)


def creta_main_category_text(main_categories) -> str:
    list_cats = []

    for main_category in main_categories:
        list_cats.append(f"[[:Category:Our World in Data - {main_category}|{main_category}]]")

    if len(list_cats) == 1:
        return list_cats[0]

    main_category_text = "/".join(list_cats)

    return f"({main_category_text})"


def slug_link(x):
    x_text = x
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


def add_category_to_pages(category_name, titles):
    category_page = page_mwclient(category_name)
    if not category_page.exists():
        print(f"Category {category_name} does not exist")
        return

    for x in tqdm(titles):
        page = page_mwclient(x)
        if page.exists():
            page_text = page.get_text()
            if category_name in page_text:
                print(f"category {category_name} already in page {x}")
                return

            text = page_text + f"\n[[{category_name}]]"
            page.save(text, f"Adding [[{category_name}]]")
            print(f"save page: {x} success.")
            return


with open(main_dir / "to_create.json", "w", encoding="utf-8") as f:
    json.dump(to_create, f, indent=4, ensure_ascii=False)

for n, (category, v) in enumerate(to_create.items(), start=1):
    print(f"{n}/{len(to_create)}: {category=}")

    sub_categories = v["sub_categories"]
    main_categories = v["main"]
    category_name = v["category_name"]

    # if len(main_categories) == 1: continue

    text = create_category_text(main_categories, category_name, sub_categories)

    to_save[category_name] = text

    page = page_mwclient(category_name)

    if page.exists():
        print(f"Category [[{category_name}]] exists")
        page.save(text, "Updating category")
    else:
        print(f"Creating Category {category_name}")
        page.create(text, "Creating category")
    # ---
    titles = get_list(list(sub_categories))
    add_category_to_pages(category_name, titles)
