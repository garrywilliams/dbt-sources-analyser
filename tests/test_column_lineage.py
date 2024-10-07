import pytest
from src.column_lineage import ColumnLineageTracer

@pytest.fixture
def sample_nodes():
    return {
        'source.raw.users': {
            'resource_type': 'source',
            'columns': {'id': {}, 'name': {}, 'email': {}}
        },
        'model.staging.stg_users': {
            'resource_type': 'model',
            'columns': {'user_id': {}, 'full_name': {}, 'email_address': {}},
            'depends_on': {'nodes': ['source.raw.users']},
            'raw_sql': "SELECT id as user_id, name as full_name, email as email_address FROM source.raw.users"
        },
        'model.mart.dim_users': {
            'resource_type': 'model',
            'columns': {'user_id': {}, 'full_name': {}, 'email': {}},
            'depends_on': {'nodes': ['model.staging.stg_users']},
            'raw_sql': "SELECT user_id, full_name, email_address as email FROM model.staging.stg_users"
        }
    }

@pytest.fixture
def tracer(sample_nodes):
    return ColumnLineageTracer(sample_nodes)

def test_trace_column_lineage(tracer):
    lineage = tracer.trace_column_lineage('model.mart.dim_users', 'user_id')
    assert lineage == {('source.raw.users', 'id')}

def test_trace_column_lineage_with_rename(tracer):
    lineage = tracer.trace_column_lineage('model.mart.dim_users', 'email')
    assert lineage == {('source.raw.users', 'email')}

def test_get_base_level_lineage(tracer):
    base_lineage = tracer.get_base_level_lineage('model.mart.dim_users')
    expected = {
        'user_id': {('source.raw.users', 'id')},
        'full_name': {('source.raw.users', 'name')},
        'email': {('source.raw.users', 'email')}
    }
    assert base_lineage == expected

def test_is_potential_rename(tracer):
    assert tracer._is_potential_rename('user_id', 'id')
    assert tracer._is_potential_rename('full_name', 'name')
    assert tracer._is_potential_rename('email_address', 'email')
    assert not tracer._is_potential_rename('user_id', 'name')