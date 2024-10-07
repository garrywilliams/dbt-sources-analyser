# sql_preprocessor.py
from jinja2 import Environment

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
    # Resolve ref and source references in SQL using the manifest
    return resolve_references(raw_sql, manifest)
