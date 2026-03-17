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


main_dir = Path(__file__).parent

# Read the original file
with open(main_dir / "topicTagGraph.json", "r", encoding="utf-8") as f:
    data = json.load(f)

# Simplify the structure
simplified_data = simplify_node(data)
simplified_data2 = simplify_node2(data)

# Write the new simplified JSON file
with open(main_dir / "topicTagGraph_simple.json", "w", encoding="utf-8") as f:
    json.dump(simplified_data, f, indent=2, ensure_ascii=False)

# Write the new simplified JSON file
with open(main_dir / "topicTagGraph_2.json", "w", encoding="utf-8") as f:
    json.dump(simplified_data2, f, indent=2, ensure_ascii=False)

print("Generated topicTagGraph_simple.json successfully!")
print("Original structure simplified to only include 'name' and 'children' fields.")
