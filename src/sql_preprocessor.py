import re
from jinja2 import Environment

def preprocess_config_blocks(sql: str) -> str:
    """Remove Jinja config blocks (e.g., {{ config(...) }})."""
    # Regex to capture and remove {{ config(...) }} blocks, including multiline content
    return re.sub(r"\{\{\s*config\s*\([\s\S]*?\)\s*\}\}", "", sql)

def resolve_references(sql: str, manifest: dict) -> str:
    """Resolve dbt ref and source references using the manifest."""
    
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
    # First remove config blocks
    preprocessed_sql = preprocess_config_blocks(raw_sql)
    
    # Then resolve ref and source references
    return resolve_references(preprocessed_sql, manifest)