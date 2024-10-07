import argparse
from src.model_analyzer import ModelAnalyzer
from src.yml_processor import process_yaml_sources
from src.manifest_loader import load_manifest


def main():
    # Set up argument parser
    parser = argparse.ArgumentParser(
        description="Analyze DBT models and report column lineage."
    )
    parser.add_argument(
        "--manifest", required=True, help="Path to the DBT manifest.json file."
    )
    parser.add_argument(
        "--report_model", required=True, help="Report model to analyze."
    )
    parser.add_argument(
        "--yaml",
        required=True,
        help="Path to the directory containing YAML source files.",
    )

    # Parse command-line arguments
    args = parser.parse_args()

    # Load the manifest.json file
    manifest_path = args.manifest
    manifest_data = load_manifest(manifest_path)

    # Process the YAML source files to extract sources
    yaml_path = args.yaml
    source_data = process_yaml_sources(yaml_path)

    # Initialize ModelAnalyzer with manifest nodes and source data
    nodes = manifest_data.get("nodes", {})  # Assuming manifest has 'nodes'
    model_analyzer = ModelAnalyzer(nodes, source_data)

    # Analyze the report model's lineage
    report_model_name = args.report_model
    print(f"Source columns for report model '{report_model_name}':")
    report_source_columns = model_analyzer.lineage_tracer.get_base_level_lineage(
        report_model_name
    )
    print(report_source_columns)

    # Analyze and display materialized models
    print("\nMaterialized models and their source columns:")
    materialized_lineage = model_analyzer.get_materialized_model_lineage()
    for model_name, lineage in materialized_lineage.items():
        print(f"Materialized Model: {model_name}")
        print(lineage)

    # Compare report model with materialized models
    print("\nModel comparison results:")
    matches = model_analyzer.analyze_model_matches(report_model_name)
    for match in matches:
        print(match)


if __name__ == "__main__":
    main()
