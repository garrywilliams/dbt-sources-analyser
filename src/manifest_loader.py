import json

def load_manifest(manifest_path: str) -> dict:
    """
    Load the DBT manifest.json file and return its contents.
    """
    try:
        with open(manifest_path, 'r') as f:
            manifest_data = json.load(f)
            return manifest_data
    except Exception as e:
        print(f"Error loading manifest file: {e}")
        return {}