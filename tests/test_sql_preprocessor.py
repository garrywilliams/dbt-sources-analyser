import pytest
from src.sql_preprocessor import preprocess_sql

@pytest.fixture
def manifest():
    return {
        'nodes': {
            'model.my_table': {'compiled_name': 'compiled_my_table'}
        },
        'sources': {
            'source.my_source.my_table': {'compiled_name': 'compiled_my_source_my_table'}
        }
    }

def test_ignore_config_block(manifest):
    sql = """
    {{ config(tags=["tag1", "tag2"]) }}
    SELECT * FROM {{ ref('my_table') }};
    """
    processed_sql = preprocess_sql(sql, manifest)
    assert "config" not in processed_sql
    assert "SELECT * FROM compiled_my_table;" in processed_sql

def test_ref_resolution(manifest):
    sql = "SELECT * FROM {{ ref('my_table') }};"
    processed_sql = preprocess_sql(sql, manifest)
    assert "SELECT * FROM compiled_my_table;" == processed_sql.strip()

def test_source_resolution(manifest):
    sql = "SELECT * FROM {{ source('my_source', 'my_table') }};"
    processed_sql = preprocess_sql(sql, manifest)
    assert "SELECT * FROM compiled_my_source_my_table;" == processed_sql.strip()

def test_ignore_var(manifest):
    sql = "SELECT * FROM {{ ref('my_table') }} WHERE date = {{ var('date') }};"
    processed_sql = preprocess_sql(sql, manifest)
    assert "SELECT * FROM compiled_my_table WHERE date = ;" == processed_sql.strip()

def test_ignore_macro(manifest):
    sql = "SELECT {{ macro('some_macro') }} FROM {{ ref('my_table') }};"
    processed_sql = preprocess_sql(sql, manifest)
    assert "SELECT  FROM compiled_my_table;" == processed_sql.strip()

def test_complex_sql(manifest):
    sql = """
    {{ config(tags=["tag1"]) }}
    {% set my_var = var('some_var', 'default') %}
    SELECT 
        {{ macro('some_macro') }},
        *
    FROM {{ ref('my_table') }}
    JOIN {{ source('my_source', 'my_table') }} USING (id)
    WHERE date = {{ var('date') }}
    """
    processed_sql = preprocess_sql(sql, manifest)
    assert "config" not in processed_sql
    assert "var" not in processed_sql
    assert "macro" not in processed_sql
    assert "compiled_my_table" in processed_sql
    assert "compiled_my_source_my_table" in processed_sql

def test_remove_macro_definition(manifest):
    sql = """
    {% macro test_macro(arg1, arg2) %}
        SELECT {{ arg1 }} + {{ arg2 }}
    {% endmacro %}

    SELECT * FROM {{ ref('my_table') }};
    """
    processed_sql = preprocess_sql(sql, manifest)
    assert "macro test_macro" not in processed_sql
    assert "SELECT * FROM compiled_my_table;" in processed_sql