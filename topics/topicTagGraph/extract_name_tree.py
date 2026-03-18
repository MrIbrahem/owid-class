import json
from pathlib import Path


def simplify_node(node):
    """
    Extract only name and children from a node.
    example of output:
    {
        "name": "tag-graph-root",
        "children": [
            {
            "name": "Population and Demographic Change",
            "children": [
                {
                "name": "Population Change",
                "children": [
                    {}
                ]
                }
            ]
            }
        ]
    }
    """
    simplified = {
        "name": node["name"],
        "children": []
    }
    if node.get("children"):
        simplified["children"] = [simplify_node(child) for child in node["children"]]
    return simplified


def simplify_node2(node):
    """
    Extract only name and children from a node as nested dict.
    example of output:
    {
        "tag-graph-root": {
            "Population and Demographic Change": {
                "Population Change": {}
            }
        }
    }
    """
    result = {}
    name = node["name"]
    if node.get("children"):
        children_dict = {}
        for child in node["children"]:
            child_result = simplify_node2(child)
            children_dict.update(child_result)
        result[name] = children_dict
    else:
        result[name] = {}
    return result


def simplify_node_id(node):
    """
    Extract name:id mapping from a node.
    example of output:
    {
        "tag-graph-root": 1837,
        "Population and Demographic Change": 1500,
        "Population Change": 1841,
        "Population Growth": 1
    }
    """
    result = {}
    result[node["name"]] = node["id"]
    if node.get("children"):
        for child in node["children"]:
            result.update(simplify_node_id(child))
    return result


main_dir = Path(__file__).parent

# Read the original file
with open(main_dir / "topicTagGraph.json", "r", encoding="utf-8") as f:
    data = json.load(f)

# Simplify the structure
simplified_data = simplify_node(data)
simplified_data2 = simplify_node2(data)
simplified_data2 = simplified_data2.get("tag-graph-root", simplified_data2)


def fltten_keys(d)-> list[str]:
    data = list(d.keys())
    for k in d:
        if isinstance(d[k], dict):
            data.extend(fltten_keys(d[k]))
    return data


simplified_data2_fletten_new = {
    # x: fltten_keys(v) for x, v in simplified_data2.items()
    x: list(v.keys()) for x, v in simplified_data2.items()
}

simplified_data2_fletten = {
    x: fltten_keys(v) for x, v in simplified_data2.items()
}

simplified_data2_fletten_reverced = {}

for x, v in simplified_data2_fletten.items():
    for y in v:
        xx = f"Category:Our World in Data - {x}"
        if y not in simplified_data2_fletten_reverced:
            simplified_data2_fletten_reverced[y] = []
        simplified_data2_fletten_reverced[y].append(xx)
        # ---
        simplified_data2_fletten_reverced[y] = list(set(simplified_data2_fletten_reverced[y]))

simplified_data_id = simplify_node_id(data)

simplified_data_id = dict(sorted(simplified_data_id.items(), key=lambda item: item[0]))

# Write the new simplified JSON file
with open(main_dir / "topicTagGraph_simple.json", "w", encoding="utf-8") as f:
    json.dump(simplified_data, f, indent=4, ensure_ascii=False)

# Write the new simplified JSON file
with open(main_dir / "topicTagGraph_2.json", "w", encoding="utf-8") as f:
    json.dump(simplified_data2, f, indent=4, ensure_ascii=False)

# Write the name:id mapping JSON file
with open(main_dir / "topicTagGraph_id.json", "w", encoding="utf-8") as f:
    json.dump(simplified_data_id, f, indent=4, ensure_ascii=False)

print("Generated topicTagGraph_simple.json successfully!")
print("Original structure simplified to only include 'name' and 'children' fields.")
print("Generated topicTagGraph_id.json successfully!")

# Write the new simplified JSON file
with open(main_dir / "simplified_data2_fletten.json", "w", encoding="utf-8") as f:
    json.dump(simplified_data2_fletten, f, indent=4, ensure_ascii=False)

# Write the new simplified JSON file
with open(main_dir / "simplified_data2_fletten_new.json", "w", encoding="utf-8") as f:
    json.dump(simplified_data2_fletten_new, f, indent=4, ensure_ascii=False)

# Write the new simplified JSON file
with open(main_dir / "simplified_data2_fletten_reverced.json", "w", encoding="utf-8") as f:
    json.dump(simplified_data2_fletten_reverced, f, indent=4, ensure_ascii=False)
