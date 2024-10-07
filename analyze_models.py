import argparse
import json
from src.model_analyzer import ModelAnalyzer
from src.yml_processor import process_yaml_files, merge_yaml_with_manifest


def load_manifest(path: str) -> dict:
    with open(path, 'r') as f:
        return json.load(f)


def main(manifest_path: str, report_model: str, yaml_path: str = None):
    # Load manifest.json file
    manifest_data = load_manifest(manifest_path)

    # Process optional YAML files if provided
    if yaml_path:
        yaml_data = process_yaml_files(yaml_path)
        manifest_data = merge_yaml_with_manifest(yaml_data, manifest_data)

    # Initialize ModelAnalyzer
    analyzer = ModelAnalyzer(manifest_data['nodes'])

    # Get the source table and columns for the report model
    source_columns = analyzer.lineage_tracer.get_base_level_lineage(report_model)

    print(f"Source columns for report model '{report_model}':")
    for column, sources in source_columns.items():
        print(f"  Column: {column}, Sources: {sources}")

    # Dump all materialized models and their source columns
    materialized_model_lineage = analyzer.get_materialized_model_lineage()

    print("\nMaterialized models and their source columns:")
    for model_name, lineage in materialized_model_lineage.items():
        print(f"Materialized Model: {model_name}")
        for column, sources in lineage.items():
            print(f"  Column: {column}, Sources: {sources}")

    # Perform the analysis for matching materialized models
    matches = analyzer.analyze_model_matches(report_model)

    print(f"\nMaterialized models matching '{report_model}':")
    for match in matches:
        print(f"  Materialized Model: {match['materialized_model']}, "
              f"Match Score: {match['match_score']}")
        print(f"  Matching Columns: {match['matching_columns']}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Analyze report model lineage and match with materialized models.")
    parser.add_argument("--manifest", required=True, help="Path to the manifest.json file")
    parser.add_argument("--report_model", required=True, help="The report model name to analyze")
    parser.add_argument("--yaml_path", help="Optional: Path to the folder containing YAML model definitions", default=None)

    args = parser.parse_args()
    main(args.manifest, args.report_model, args.yaml_path)
