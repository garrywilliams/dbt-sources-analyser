# test_sql_preprocessor.py

import pytest
from src.sql_preprocessor import preprocess_sql

@pytest.fixture
def manifest():
    return {
        'nodes': {
            'model.my_table': {'compiled_name': 'compiled_my_table'}
        },
        'sources': {}
    }

def test_ignore_config_block(manifest):
    sql = """
    {{ config(
        tags=["tag1", "tag2"]
    ) }}
    SELECT * FROM {{ ref("my_table") }};
    """
    
    processed_sql = preprocess_sql(sql, manifest)
    
    assert "config" not in processed_sql  # Ensure config block is removed
    assert "SELECT * FROM compiled_my_table;" in processed_sql  # Ensure reference is resolved

def test_ref_resolution(manifest):
    sql = "SELECT * FROM {{ ref('my_table') }};"
    processed_sql = preprocess_sql(sql, manifest)
    
    assert "SELECT * FROM compiled_my_table;" == processed_sql

def test_source_resolution(manifest):
    manifest['sources'] = {
        'source.my_source.my_table': {'compiled_name': 'compiled_my_source_my_table'}
    }
    sql = "SELECT * FROM {{ source('my_source', 'my_table') }};"
    processed_sql = preprocess_sql(sql, manifest)
    
    assert "SELECT * FROM compiled_my_source_my_table;" == processed_sql
