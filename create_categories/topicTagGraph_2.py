

import functools
# import sys
import os
import json
import mwclient
from tqdm import tqdm
from pathlib import Path
from dotenv import load_dotenv
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


def create_category_text(main_category, category, sub_categories) -> str:
    text = []

    text.append(f"[[:Category:Our World in Data topics|Our World in Data topics]] > [[:Category:Our World in Data - {main_category}|{main_category}]] > [[:Category:Our World in Data - {category}|{category}]]:")
    text.append("\nTopics in this category:")

    for x in sub_categories:
        x_text = slug_link(x)
        text.append(f"* {x_text}")

    text.append(f"[[Category:Our World in Data - {main_category}| ]]")

    return "\n".join(text)


def slug_link(x):
    x_text = x
    if ids_slug_data.get(x, {}).get("slug"):
        x_text = f"[https://ourworldindata.org/{ids_slug_data[x]['slug']} {x}]"
    return x_text


for x, v in tqdm(data_list.items()):
    for category, sub_categories in v.items():
        text = create_category_text(x, category, sub_categories)
        category_name = f"Category:Our World in Data - {category}"
        to_save[category_name] = text

        page = page_mwclient(category_name)

        if page.exists():
            print(f"Category {category_name} exists")
            page.save(text, "Updating category")
        else:
            print(f"Creating Category {category_name}")
            page.create(text, "Creating category")
