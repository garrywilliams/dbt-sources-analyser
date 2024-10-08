from jinja2 import Environment, Undefined, nodes
from jinja2.visitor import NodeTransformer
import re

class IgnoreUndefined(Undefined):
    def __str__(self):
        return f'"PLACEHOLDER_{self._undefined_name}"'

    def __call__(self, *args, **kwargs):
        return f'"PLACEHOLDER_{self._undefined_name}"'

class ConfigMacroAndSetRemover(NodeTransformer):
    def visit_Call(self, node):
        if isinstance(node.node, nodes.Name) and node.node.name == 'config':
            return nodes.Const('')
        return node

    def visit_Macro(self, node):
        return None

    def visit_Assign(self, node):
        if isinstance(node.target, nodes.Name):
            return None
        return node

class CustomEnvironment(Environment):
    def _parse(self, source, name, filename):
        parsed = super()._parse(source, name, filename)
        transformer = ConfigMacroAndSetRemover()
        return transformer.visit(parsed)

def resolve_references(sql: str, manifest: dict):
    def ref(model_name):
        return manifest['nodes'].get(f'model.{model_name}', {}).get('compiled_name', model_name)

    def source(source_name, table_name):
        return manifest['sources'].get(f'source.{source_name}.{table_name}', {}).get('compiled_name', f'{source_name}.{table_name}')

    env = CustomEnvironment(undefined=IgnoreUndefined)
    env.globals['ref'] = ref
    env.globals['source'] = source
    env.globals['config'] = lambda *args, **kwargs: ''
    env.globals['var'] = lambda *args, **kwargs: '"PLACEHOLDER_var"'
    env.globals['macro'] = lambda *args, **kwargs: ''

    try:
        rendered_sql = env.from_string(sql).render()
        return rendered_sql
    except Exception as e:
        print(f"Error processing SQL: {e}")
        return sql

def preprocess_sql(raw_sql: str, manifest: dict) -> str:
    return resolve_references(raw_sql, manifest)