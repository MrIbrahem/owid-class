import json
from pathlib import Path


def simplify_node(node):
    """Extract only name and children from a node."""
    simplified = {
        "name": node["name"],
        "children": []
    }
    if node.get("children"):
        simplified["children"] = [simplify_node(child) for child in node["children"]]
    return simplified


main_dir = Path(__file__).parent

# Read the original file
with open(main_dir / "topicTagGraph.json", "r", encoding="utf-8") as f:
    data = json.load(f)

# Simplify the structure
simplified_data = simplify_node(data)

# Write the new simplified JSON file
with open(main_dir / "topicTagGraph_simple.json", "w", encoding="utf-8") as f:
    json.dump(simplified_data, f, indent=2, ensure_ascii=False)

print("Generated topicTagGraph_simple.json successfully!")
print("Original structure simplified to only include 'name' and 'children' fields.")
