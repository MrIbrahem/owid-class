

import json
import mwclient
from pathlib import Path

site = mwclient.Site('commons.wikimedia.org')
site.login(username, password)

main_dir = Path(__file__).parent
file_path = main_dir / "topicTagGraph_2.json"

data_list = json.loads(file_path.read_text(encoding="utf-8"))

to_save = {}


def create_category_text(main_category, sub_categories) -> str:
    text = []
    text.append("Topics in this category:")
    for x in sub_categories:
        text.append(f"* {x}")
    text.append(f"[[Category:Our World in Data - {main_category}]]")

    return "\n".join(text)


for x, v in data_list.items():
    for category, sub_categories in v.items():
        text = create_category_text(x, sub_categories)
        category_name = f"Category:Our World in Data - {category}"
        to_save[category_name] = text
