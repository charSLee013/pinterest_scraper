import ijson
import json

def infer_json_structure(file_path, sample_size=5):
    """
    Infer the structure of a large JSON file by sampling a few items.
    Assumes the top-level structure is a list of objects.
    """
    structure = {}
    with open(file_path, 'rb') as f:
        try:
            # Try to parse the top-level as an array of items
            items_iterator = ijson.items(f, 'item')
            for i, record in enumerate(items_iterator):
                if i >= sample_size:
                    break
                _infer_object_structure(record, structure)
            
            if not structure:
                # If the above didn't yield any structure, it might be a single JSON object at the root
                f.seek(0) # Reset file pointer
                obj_iterator = ijson.items(f, '') # Parse the entire object if it's a single root object
                for i, record in enumerate(obj_iterator):
                    if i >= sample_size: # In this case, sample_size applies to top-level object keys
                        break
                    _infer_object_structure(record, structure)


        except ijson.common.JSONError as e:
            print(f"Error parsing with ijson: {e}. Attempting to load as a single JSON object.")
            # Fallback if the top-level is not an array or 'item' path is invalid
            f.seek(0)  # Reset file pointer
            try:
                # Try to load as a single JSON object
                single_object = json.load(f)
                _infer_object_structure(single_object, structure)
            except json.JSONDecodeError as de:
                print(f"Could not decode JSON: {de}")
                return {"error": "Could not parse JSON file."}

    return structure

def _infer_object_structure(obj_dict, current_structure_dict):
    """
    Infers the structure of a dictionary object.
    obj_dict: The dictionary object to infer.
    current_structure_dict: The dictionary where the inferred structure of obj_dict's keys will be stored.
    """
    for key, value in obj_dict.items():
        if key not in current_structure_dict:
            current_structure_dict[key] = {}
        # Call _infer_value_structure to populate the specific key's structure
        _infer_value_structure(value, current_structure_dict[key])

def _infer_value_structure(value, node_structure_dict):
    """
    Infers the structure of a single value and updates node_structure_dict.
    node_structure_dict: The dictionary representing the current node's structure (e.g., for a key in a dict, or an item in an array).
                        This dict will store 'type', '_structure' (if dict), or '_array_item_structure' (if list).
    """
    # Ensure 'type' key exists and is a set
    if 'type' not in node_structure_dict:
        node_structure_dict['type'] = set()

    if isinstance(value, dict):
        node_structure_dict['type'].add('dict')
        if '_structure' not in node_structure_dict:
            node_structure_dict['_structure'] = {}
        _infer_object_structure(value, node_structure_dict['_structure']) # Pass the nested _structure dict
    elif isinstance(value, list):
        node_structure_dict['type'].add('list')
        if '_array_item_structure' not in node_structure_dict:
            node_structure_dict['_array_item_structure'] = {}
        if value:
            # Infer structure of the first element of the list, populating _array_item_structure
            _infer_value_structure(value[0], node_structure_dict['_array_item_structure'])
        else:
            # For empty lists, the _array_item_structure remains an empty dict, indicating no observed items.
            node_structure_dict['_array_item_structure'] = {}
    elif isinstance(value, (str, int, float, bool, type(None))):
        type_name = str(type(value).__name__)
        node_structure_dict['type'].add(type_name)


if __name__ == "__main__":
    file_path = '/Users/charslee/Repo/private/pinterest_scraper/debug_responses/response_page_1.txt'
    print(f"Inferring structure for {file_path}...")
    json_structure = infer_json_structure(file_path)
    # Convert sets to lists for better readability in output
    def convert_sets_to_lists(obj):
        if isinstance(obj, dict):
            return {k: convert_sets_to_lists(v) for k, v in obj.items()}
        elif isinstance(obj, set):
            return list(obj)
        else:
            return obj

    readable_structure = convert_sets_to_lists(json_structure)
    print(json.dumps(readable_structure, indent=4)) 