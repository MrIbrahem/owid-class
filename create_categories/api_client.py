

import functools
import jsonlines
import sys
import os
import mwclient
from pathlib import Path
from tqdm import tqdm
from dotenv import load_dotenv

not_exists_pages = []
exists_pages = []

exists_pages_file = Path(__file__).parent / "exists_pages.jsonl"

if exists_pages_file.exists():
    with jsonlines.open(exists_pages_file, "r") as f:
        for obj in f:
            if obj["exists"]:
                exists_pages.append(obj["title"])
            else:
                not_exists_pages.append(obj["title"])

load_dotenv()

global_saves = {1: 0}


def save_title_to_jsonl(title: str, exists) -> None:
    if title.startswith("OWID/"):
        with jsonlines.open(exists_pages_file, "a") as f:
            f.write({"title": title, "exists": exists})


def get_global_saves() -> int:
    return global_saves[1]


@functools.lru_cache(maxsize=1)
def initialize_site_connection():
    username = os.environ.get("WIKI_USERNAME")
    password = os.environ.get("WIKI_PASSWORD")

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

        self.site_mw = initialize_site_connection()

        self.page = self.site_mw.pages[title]

    def get_text(self):
        return self.page.text()

    def exists(self) -> bool:
        if self.title in exists_pages:
            return True

        exists: bool = self.page.exists

        if exists:
            exists_pages.append(self.title)
        else:
            not_exists_pages.append(self.title)

        save_title_to_jsonl(self.title, exists)
        return exists

    def ask(self, summary):
        if "ask" in sys.argv:
            ask = input(f"Do you want to save the changes? (y/n): {summary=}")
            yess = ["", "y", "a"]
            if ask not in yess:
                return False
        return True

    def save(self, newtext: str, summary: str):
        if not self.ask(summary):
            return False

        page_text = self.get_text()
        if page_text == newtext:
            print("No changes..")
            return False

        result = self.page.save(newtext, summary=summary)
        print(f"Saved page {self.title} with result: {result}")

        global_saves[1] += 1
        return result

    def create(self, newtext: str, summary: str):
        if not self.ask(summary):
            return False

        result = self.page.save(newtext, summary=summary)
        print(f"Created page {self.title} with result: {result}")

        global_saves[1] += 1
        return result


@functools.lru_cache(maxsize=1)
def get_page(title: str) -> page_mwclient:
    return page_mwclient(title)


def filter_titles(titles) -> list:
    titles_exists = [x for x in titles if x in exists_pages]
    if titles_exists:
        print(f"titles_exists: {len(titles_exists)}, all titles: {len(titles)}")
        titles = titles_exists
    else:
        titles_not_exists = [x for x in titles if x in not_exists_pages]
        if titles_not_exists:
            print(f"titles_not_exists: {len(titles_not_exists)}, all titles: {len(titles)}")
            titles = [x for x in titles if x not in titles_not_exists]
    return titles


def get_exists_pages(titles) -> list[page_mwclient]:
    print(f"get_exists_pages, titles: {len(titles)}")

    titles = filter_titles(titles)
    exists_pages = []
    for x in tqdm(titles):
        page = get_page(x)
        if page.exists():
            exists_pages.append(page)
    return []


def add_category_to_pages(category_name: str, titles: list[page_mwclient]) -> None:
    category_page = get_page(category_name)
    if not category_page.exists():
        print(f"Category {category_name} does not exist")
        return

    print(f"add_category_to_pages, titles: {len(titles)}")

    for page in tqdm(titles):
        page_text = page.get_text()
        if category_name in page_text:
            print(f"category {category_name} already in page {page.title}")
            continue

        text = page_text + f"\n[[{category_name}]]"
        page.save(text, f"Adding [[{category_name}]]")
        print(f"save page: {page.title} success.")


def create_category(category_name, text):
    page = get_page(category_name)

    if not page.exists():
        print(f"Creating Category {category_name}")
        page.create(text, "Creating category")
        return

    print(f"Category [[{category_name}]] exists")
    page.save(text, "Updating category")
