import yaml
import os
from typing import Dict

def process_yaml_files(project_dir: str) -> Dict[str, Dict]:
    yaml_data = {}
    for root, dirs, files in os.walk(project_dir):
        for file in files:
            if file.endswith('.yml') or file.endswith('.yaml'):
                with open(os.path.join(root, file), 'r') as f:
                    data = yaml.safe_load(f)
                    if isinstance(data, dict):
                        yaml_data.update(data)
    return yaml_data

def merge_yaml_with_manifest(yaml_data: Dict, manifest_data: Dict) -> Dict:
    merged_data = manifest_data.copy()
    for node_name, node_data in merged_data['nodes'].items():
        if node_name in yaml_data:
            node_data.update(yaml_data[node_name])
    return merged_data