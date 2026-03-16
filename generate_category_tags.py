import json

def extract_leaf_tags(node):
    """Recursively extract all leaf tags (nodes with slug) from a tree node."""
    leaves = []
    if node.get('slug'):
        leaves.append({
            'id': node['id'],
            'name': node['name'],
            'slug': node['slug']
        })
    for child in node.get('children', []):
        leaves.extend(extract_leaf_tags(child))
    return leaves

def main():
    # Load tags_categories.json to get category keys
    with open('tags_categories.json', 'r', encoding='utf-8') as f:
        tags_categories = json.load(f)

    # Load topicTagGraph.json
    with open('topicTagGraph.json', 'r', encoding='utf-8') as f:
        topic_tag_graph = json.load(f)

    # Build mapping from category name to its tags
    result = {}

    # Get root children (the main categories in the topic graph)
    root_children = topic_tag_graph.get('children', [])

    for category_key, category_value in tags_categories.items():
        # Extract category name from the key (e.g., "Education and Knowledge")
        category_name = category_key

        # Find matching category in topic graph
        tags = []
        for category_node in root_children:
            if category_node.get('name') == category_name:
                # Extract all leaf tags from this category
                tags = extract_leaf_tags(category_node)
                break

        # Map to the category value from tags_categories.json
        result[category_value] = tags

    # Write output
    with open('category_tags_mapping.json', 'w', encoding='utf-8') as f:
        json.dump(result, f, indent=4, ensure_ascii=False)

    print("Generated category_tags_mapping.json successfully!")

    # Print summary
    for category, tags in result.items():
        print(f"  {category}: {len(tags)} tags")

if __name__ == '__main__':
    main()
