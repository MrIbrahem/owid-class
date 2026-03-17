import json
from pathlib import Path


def filter_json_structure(data):
    """
    Recursively filters the dictionary to keep only 'name' and 'children' keys.
    """
    # Create a new dictionary with only the desired keys if they exist
    filtered_item = {}

    if "name" in data:
        filtered_item["name"] = data["name"]

    if "children" in data:
        # Recursively apply this filter to every child in the list
        filtered_item["children"] = [filter_json_structure(child) for child in data["children"]]

    return filtered_item


def process_file(input_filename, output_filename):
    main_dir = Path(__file__).parent
    try:
        # Load the original JSON data
        with open(main_dir / input_filename, 'r', encoding='utf-8') as f:
            original_data = json.load(f)

        # Filter the data
        new_data = filter_json_structure(original_data)

        # Save the filtered data to a new file
        with open(main_dir / output_filename, 'w', encoding='utf-8') as f:
            json.dump(new_data, f, indent=4)

        print(f"Successfully created: {output_filename}")

    except FileNotFoundError:
        print(f"Error: The file {input_filename} was not found.")
    except json.JSONDecodeError:
        print(f"Error: Failed to decode JSON from {input_filename}.")


# Execute the process
process_file('topicTagGraph.json', 'filtered_topic_graph.json')
