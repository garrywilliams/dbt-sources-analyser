from typing import List, Dict
from collections import defaultdict
import sqlglot
from sqlglot import exp

class SqlParser:
    @staticmethod
    def extract_table_references(sql: str) -> List[str]:
        parsed = sqlglot.parse_one(sql)
        tables = [str(table) for table in parsed.find_all(exp.Table)]
        return tables

    @staticmethod
    def parse_joins(sql: str) -> List[tuple]:
        parsed = sqlglot.parse_one(sql)
        joins = []
        for join in parsed.find_all(exp.Join):
            join_type = join.args.get('side', 'INNER').upper()
            joined_table = str(join.args['this'])
            join_condition = str(join.args.get('on', None))
            joins.append((f"{join_type} JOIN", joined_table, join_condition))
        return joins

    @staticmethod
    def extract_transformations(sql: str) -> Dict[str, List[Dict[str, str]]]:
        parsed = sqlglot.parse_one(sql)
        transformations = defaultdict(list)

        # Traverse the parsed SQL expression tree
        for expr in parsed.expressions:
            SqlParser._process_expression(expr, transformations)

        return dict(transformations)

    @staticmethod
    def _process_expression(expr, transformations):
        if isinstance(expr, exp.Func):
            # Directly process SQL functions like CONCAT, UPPER, etc.
            SqlParser._process_function(expr, transformations)
        elif isinstance(expr, exp.Binary):
            # Handle binary operations like age + 1
            SqlParser._process_operation(expr, transformations)
        elif isinstance(expr, exp.Alias):
            # Handle aliases (e.g., CONCAT(first_name, ' ', last_name) AS full_name)
            alias = str(expr.args['alias'])
            original = str(expr.args['this'])
            transformations[alias].append({
                'type': 'rename',
                'original': original,
                'alias': alias
            })
            SqlParser._process_expression(expr.args['this'], transformations)
        elif isinstance(expr, exp.Column):
            # Handle columns directly
            column = str(expr)
            if column not in transformations:
                transformations[column].append({
                    'type': 'column',
                    'name': column
                })

    @staticmethod
    def _process_function(func, transformations):
        function_name = func.key.upper()
        alias = str(func.parent.args['alias']) if isinstance(func.parent, exp.Alias) else None

        # Process each argument in the function call, including handling lists of arguments
        for arg in func.args.values():
            if isinstance(arg, list):
                # If the argument is a list (like in CONCAT), process each element
                for sub_arg in arg:
                    SqlParser._process_function_argument(sub_arg, function_name, alias, transformations)
            else:
                SqlParser._process_function_argument(arg, function_name, alias, transformations)

        # Handle the alias of the function, if it exists
        if alias:
            original_expr = str(func)
            transformations[alias].append({
                'type': 'rename',
                'original': original_expr,
                'alias': alias
            })

    @staticmethod
    def _process_function_argument(arg, function_name, alias, transformations):
        # Handle column arguments within a function
        if isinstance(arg, exp.Column):
            column = str(arg)
            transformations[column].append({
                'type': 'function',
                'function': function_name,
                'arguments': column,
                'alias': alias or column  # Use alias if present, else column name
            })
        elif isinstance(arg, exp.Func):
            # If the argument itself is a function, recursively process the function
            SqlParser._process_function(arg, transformations)
        else:
            # Process non-column arguments (e.g., constants or literals)
            print(f"Found non-column argument: {arg}")  # Diagnostic for non-column arguments



    @staticmethod
    def _process_operation(op, transformations):
        left = str(op.args['this'])
        right = str(op.args['expression'])
        operator = op.key.upper()
        transformations[left].append({
            'type': 'operation',
            'operator': operator,
            'right_operand': right,
            'alias': str(op.parent.args['alias']) if isinstance(op.parent, exp.Alias) else left
        })


    @staticmethod
    def analyze_column_lineage(sql: str) -> Dict[str, Dict[str, List[str]]]:
        parsed = sqlglot.parse_one(sql)
        lineage = defaultdict(lambda: defaultdict(list))
        for expr in parsed.find_all(exp.Expression):
            if isinstance(expr, exp.EQ):
                left = str(expr.args['this'])
                right = str(expr.args['expression'])
                if '.' in right:
                    lineage[left]['sources'].append(right)
                elif '.' in left:
                    lineage[right]['derived'].append(left)
                else:
                    lineage[left]['sources'].append(right)
                    lineage[right]['derived'].append(left)
        return dict(lineage)
