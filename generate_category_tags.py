import json
from collections import defaultdict

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

    # Track tag ID occurrences across all categories
    tag_id_to_categories = defaultdict(list)
    category_tags = {}

    # First pass: collect tags for all categories and track duplicates
    for category_key, category_value in tags_categories.items():
        category_name = category_key

        # Skip "Other" for now
        if category_key == "Other":
            continue

        # Find matching category in topic graph
        tags = []
        for category_node in root_children:
            if category_node.get('name') == category_name:
                tags = extract_leaf_tags(category_node)
                break

        category_tags[category_value] = tags

        # Track which categories each tag appears in
        for tag in tags:
            tag_id_to_categories[tag['id']].append(category_value)

    # Find duplicate tag IDs (tags that appear in multiple categories)
    duplicate_tag_ids = {tag_id for tag_id, cats in tag_id_to_categories.items() if len(cats) > 1}

    # Build final result, removing duplicates from regular categories
    for category_value, tags in category_tags.items():
        # Filter out duplicate tags
        unique_tags = [tag for tag in tags if tag['id'] not in duplicate_tag_ids]
        result[category_value] = unique_tags

    # Collect all duplicate tags for "Other" category
    other_tags = []
    seen_ids = set()
    for category_node in root_children:
        tags = extract_leaf_tags(category_node)
        for tag in tags:
            if tag['id'] in duplicate_tag_ids and tag['id'] not in seen_ids:
                other_tags.append(tag)
                seen_ids.add(tag['id'])

    # Add "Other" category with duplicate tags
    other_category_value = tags_categories.get("Other")
    if other_category_value:
        result[other_category_value] = other_tags

    # Write output
    with open('category_tags_mapping.json', 'w', encoding='utf-8') as f:
        json.dump(result, f, indent=4, ensure_ascii=False)

    print("Generated category_tags_mapping.json successfully!")

    # Print summary
    for category, tags in result.items():
        print(f"  {category}: {len(tags)} tags")

if __name__ == '__main__':
    main()
