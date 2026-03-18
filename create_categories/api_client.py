

import functools
import sys
import os
import mwclient
from tqdm import tqdm
from dotenv import load_dotenv

not_exists_pages = []
exists_pages = []

load_dotenv()

global_saves = {1: 0}


def get_global_saves() -> int:
    return global_saves[1]


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
        exists = self.page.exists
        if exists:
            exists_pages.append(self.title)
        else:
            not_exists_pages.append(self.title)
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


def get_exists_pages(titles) -> list:
    print(f"add_category_to_pages, titles: {len(titles)}")

    titles = filter_titles(titles)

    for x in tqdm(titles):
        page = page_mwclient(x)
        if page.exists():
            return [x]
    return []


def add_category_to_pages(category_name, titles) -> None:
    category_page = page_mwclient(category_name)
    if not category_page.exists():
        print(f"Category {category_name} does not exist")
        return

    print(f"add_category_to_pages, titles: {len(titles)}")

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


def create_category(category_name, text):
    page = page_mwclient(category_name)

    if page.exists():
        print(f"Category [[{category_name}]] exists")
        page.save(text, "Updating category")
    else:
        print(f"Creating Category {category_name}")
        page.create(text, "Creating category")
