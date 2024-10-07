from typing import Dict, Set, Tuple
from src.sql_parser import SqlParser

class ColumnLineageTracer:
    def __init__(self, nodes: Dict):
        self.nodes = nodes
        self.sql_parser = SqlParser()

    def trace_column_lineage(self, node_name: str, column_name: str, visited: Set[str] = None) -> Set[Tuple[str, str]]:
        if visited is None:
            visited = set()

        if node_name in visited:
            return set()

        visited.add(node_name)
        node = self.nodes[node_name]
        
        if node['resource_type'] == 'source':
            return {(node_name, column_name)}

        lineage = set()
        raw_sql = node.get('raw_sql')
        if raw_sql:
            sql_lineage = self.sql_parser.analyze_column_lineage(raw_sql)
            transformations = self.sql_parser.extract_transformations(raw_sql)
            
            if column_name in sql_lineage:
                for source in sql_lineage[column_name]['sources']:
                    lineage.update(self.trace_column_lineage(node_name, source, visited.copy()))
            
            if column_name in transformations:
                for transform in transformations[column_name]:
                    if transform['type'] == 'rename':
                        lineage.update(self.trace_column_lineage(node_name, transform['original'], visited.copy()))

        for dep_name in node.get('depends_on', {}).get('nodes', []):
            dep_node = self.nodes.get(dep_name)
            if dep_node is None:
                continue
            
            if column_name in dep_node.get('columns', {}):
                lineage.update(self.trace_column_lineage(dep_name, column_name, visited.copy()))
            else:
                for dep_column in dep_node.get('columns', {}):
                    if self._is_potential_rename(column_name, dep_column):
                        lineage.update(self.trace_column_lineage(dep_name, dep_column, visited.copy()))

        return lineage

    def _is_potential_rename(self, col1: str, col2: str) -> bool:
        col1_parts = col1.lower().split('_')
        col2_parts = col2.lower().split('_')
        return any(part in col2_parts for part in col1_parts) or any(part in col1_parts for part in col2_parts)

    def get_base_level_lineage(self, node_name: str) -> Dict[str, Set[Tuple[str, str]]]:
        node = self.nodes[node_name]
        base_lineage = {}
        for column in node.get('columns', {}):
            lineage = self.trace_column_lineage(node_name, column)
            if lineage:
                base_lineage[column] = lineage
        return base_lineage