from jinja2 import Environment, nodes
from jinja2.ext import Extension

# Custom Jinja2 Extension to ignore specific blocks
class IgnoreConfigExtension(Extension):
    # A list of block names we want to ignore
    tags = {'config'}

    def parse(self, parser):
        lineno = next(parser.stream).lineno
        # Parse the block and return an empty node (effectively removing it)
        return nodes.Output([nodes.Const('')]).set_lineno(lineno)

def resolve_references(sql: str, manifest: dict):
    def ref(model_name):
        """Resolve dbt ref."""
        return manifest['nodes'].get(f'model.{model_name}', {}).get('compiled_name', model_name)

    def source(source_name, table_name):
        """Resolve dbt source."""
        return manifest['sources'].get(f'source.{source_name}.{table_name}', {}).get('compiled_name', f'{source_name}.{table_name}')

    # Create a Jinja environment and pass ref and source functions as globals
    env = Environment()
    env.globals['ref'] = ref
    env.globals['source'] = source

    # Render the SQL with Jinja to resolve references
    return env.from_string(sql).render()

def preprocess_sql(raw_sql: str, manifest: dict) -> str:
    # Resolve references and remove config blocks
    return resolve_references(raw_sql, manifest)

