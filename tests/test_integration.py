import json
import pytest
from src.column_lineage import ColumnLineageTracer
from src.model_analyzer import ModelAnalyzer

@pytest.fixture
def nodes():
    with open('tests/dummy_manifest.json', 'r') as f:
        manifest = json.load(f)
    return manifest['nodes']

def test_column_lineage_integration(nodes):
    tracer = ColumnLineageTracer(nodes, source_data={})  # Pass an empty dict for source_data    
    lineage = tracer.get_base_level_lineage('model.marts.dim_customers')
    expected = {
        'customer_id': {('source.raw.customers', 'id')},
        'full_name': {('source.raw.customers', 'name')},
        'email_address': {('source.raw.customers', 'email')}
    }
    assert lineage == expected

def test_model_analyzer_integration(nodes):
    analyzer = ModelAnalyzer(nodes)
    matches = analyzer.analyze_model_matches('model.marts.fct_orders')
    
    print("\nDetailed match information:")
    for match in matches:
        print(f"Match: {match}")
        print(f"Matching columns: {match['matching_columns']}")
        
        # Print lineage for fct_orders
        fct_lineage = analyzer.lineage_tracer.get_base_level_lineage('model.marts.fct_orders')
        print(f"fct_orders lineage: {fct_lineage}")
        
        # Print lineage for the matched model
        matched_model_lineage = analyzer.lineage_tracer.get_base_level_lineage(match['materialized_model'])
        print(f"Matched model lineage: {matched_model_lineage}")
    
    assert len(matches) == 1, f"Expected 1 match, but got {len(matches)}"
    assert matches[0]['materialized_model'] == 'model.marts.dim_customers'
    assert matches[0]['match_score'] == '1 out of 4', f"Expected '1 out of 4', but got {matches[0]['match_score']}"
    assert matches[0]['matching_columns'] == [('customer_id', 'customer_id')], f"Expected [('customer_id', 'customer_id')], but got {matches[0]['matching_columns']}"