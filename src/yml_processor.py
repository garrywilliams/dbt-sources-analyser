import yaml
import os
from typing import Dict


def process_yaml_files(project_dir: str) -> Dict[str, Dict]:
    yaml_data = {}
    for root, dirs, files in os.walk(project_dir):
        for file in files:
            if file.endswith(".yml") or file.endswith(".yaml"):
                with open(os.path.join(root, file), "r") as f:
                    data = yaml.safe_load(f)
                    if isinstance(data, dict):
                        yaml_data.update(data)
    return yaml_data


def merge_yaml_with_manifest(yaml_data: Dict, manifest_data: Dict) -> Dict:
    merged_data = manifest_data.copy()
    for node_name, node_data in merged_data["nodes"].items():
        if node_name in yaml_data:
            node_data.update(yaml_data[node_name])
    return merged_data


def process_yaml_sources(yaml_path: str) -> dict:
    """
    Parse YAML files from the given path and extract source table and column definitions.
    """
    sources = {}

    # Traverse the directory for YAML files
    for root, _, files in os.walk(yaml_path):
        for file in files:
            if file.endswith(".yml") or file.endswith(".yaml"):
                full_path = os.path.join(root, file)
                try:
                    with open(full_path, "r") as f:
                        yaml_data = yaml.safe_load(f)

                        # Check if it’s a valid source definition file
                        if "sources" in yaml_data:
                            for source in yaml_data["sources"]:
                                group_name = source["name"]
                                for table in source.get("tables", []):
                                    table_name = table["name"]
                                    columns = [
                                        col["name"] for col in table.get("columns", [])
                                    ]

                                    # Store the source table information
                                    sources[table_name] = {
                                        "group": group_name,
                                        "columns": columns,
                                    }

                except yaml.YAMLError as exc:
                    print(f"Error parsing YAML file: {full_path}")
                    print(f"YAML error details: {exc}")
                    # Optionally, continue to the next file, or stop execution based on your needs
                    continue

    return sources
