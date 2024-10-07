from jinja2 import Environment, nodes, Undefined
from jinja2.ext import Extension

# Custom Jinja2 Extension to ignore specific blocks
class IgnoreConfigExtension(Extension):
    # A list of block names we want to ignore (such as 'config')
    tags = {'config'}

    def parse(self, parser):
        lineno = next(parser.stream).lineno
        # Parse the block and return an empty node (effectively removing it)
        return nodes.Output([nodes.Const('')]).set_lineno(lineno)

# Custom Undefined class to safely ignore undefined variables like 'config'
class SilentUndefined(Undefined):
    def _fail_with_undefined_error(self, *args, **kwargs):
        return ''

# Function to resolve dbt references and sources using Jinja2
def resolve_references(sql: str, manifest: dict) -> str:
    def ref(model_name):
        """Resolve dbt ref."""
        return manifest['nodes'].get(f'model.{model_name}', {}).get('compiled_name', model_name)

    def source(source_name, table_name):
        """Resolve dbt source."""
        return manifest['sources'].get(f'source.{source_name}.{table_name}', {}).get('compiled_name', f'{source_name}.{table_name}')

    # Create a Jinja environment with the IgnoreConfigExtension and SilentUndefined
    env = Environment(extensions=[IgnoreConfigExtension], undefined=SilentUndefined)
    env.globals['ref'] = ref
    env.globals['source'] = source

    # Render the SQL with Jinja to resolve references and ignore config blocks
    return env.from_string(sql).render()

def preprocess_sql(raw_sql: str, manifest: dict) -> str:
    # Resolve references and remove config blocks
    return resolve_references(raw_sql, manifest)


