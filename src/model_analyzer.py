from typing import Dict, List
from src.column_lineage import ColumnLineageTracer

class ModelAnalyzer:
    def __init__(self, nodes: Dict, source_data: Dict = None):
        self.nodes = nodes
        self.source_data = source_data if source_data else {}
        self.lineage_tracer = ColumnLineageTracer(nodes, self.source_data)
        self.materialized_models = set(
            node_name for node_name, node in nodes.items()
            if node.get('resource_type') == 'model' and node.get('config', {}).get('materialized') == 'table'
        )

    def get_materialized_model_lineage(self) -> Dict[str, Dict]:
        """
        Returns a mapping of materialized models and their column source lineages.
        """
        model_lineages = {}
        for model_name in self.materialized_models:
            base_lineage = self.lineage_tracer.get_base_level_lineage(model_name)
            model_lineages[model_name] = base_lineage
        return model_lineages

    def analyze_model_matches(self, report_model_name: str) -> List[Dict]:
        """
        Compares a report model's column lineages with materialized models and scores matches.
        """
        report_node = self.nodes.get(report_model_name)
        if not report_node:
            return []  # Return empty list if report model doesn't exist

        report_columns = report_node.get('columns', {})
        column_lineages = self.lineage_tracer.get_base_level_lineage(report_model_name)

        matches = []
        for materialized_model in self.materialized_models:
            if materialized_model == report_model_name:
                continue

            match_count = 0
            total_columns = len(report_columns)
            matching_columns = []

            materialized_lineages = self.lineage_tracer.get_base_level_lineage(materialized_model)

            for column, lineage in column_lineages.items():
                for mat_column, mat_lineage in materialized_lineages.items():
                    if lineage.intersection(mat_lineage) and self._is_column_match(column, mat_column):
                        match_count += 1
                        matching_columns.append((column, mat_column))
                        break

            if match_count > 0:
                matches.append({
                    'report_model': report_model_name,
                    'materialized_model': materialized_model,
                    'match_score': f"{match_count} out of {total_columns}",
                    'matching_columns': matching_columns
                })

        return sorted(matches, key=lambda x: int(x['match_score'].split()[0]), reverse=True)

    def _is_column_match(self, col1: str, col2: str) -> bool:
        return col1.lower() == col2.lower() or col1.lower() in col2.lower() or col2.lower() in col1.lower()
