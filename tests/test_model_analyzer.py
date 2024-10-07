import pytest
from src.model_analyzer import ModelAnalyzer

@pytest.fixture
def mock_nodes():
    return {
        "model.report_model_1": {
            "resource_type": "model",
            "columns": {"a": {}, "b": {}, "c": {}},
            "raw_sql": "SELECT a, b, c FROM source_model_1",
            "depends_on": {"nodes": ["source_model_1", "source_model_2"]}
        },
        "model.materialized_model_1": {
            "resource_type": "model",
            "config": {"materialized": "table"},
            "columns": {"a": {}, "b": {}, "d": {}},
            "raw_sql": "SELECT a, b, d FROM source_model_1",
            "depends_on": {"nodes": ["source_model_1"]}
        },
        "source_model_1": {
            "resource_type": "source",
            "columns": {"a": {}, "b": {}, "c": {}}
        },
        "source_model_2": {
            "resource_type": "source",
            "columns": {"c": {}, "d": {}}
        }
    }

@pytest.fixture
def model_analyzer(mock_nodes):
    return ModelAnalyzer(mock_nodes, source_data={})  # Empty source_data

def test_get_materialized_model_lineage(model_analyzer):
    materialized_lineage = model_analyzer.get_materialized_model_lineage()
    assert "model.materialized_model_1" in materialized_lineage
    assert "a" in materialized_lineage["model.materialized_model_1"]
    assert "b" in materialized_lineage["model.materialized_model_1"]

def test_analyze_model_matches(model_analyzer):
    matches = model_analyzer.analyze_model_matches("model.report_model_1")
    assert len(matches) == 1
    assert matches[0]["materialized_model"] == "model.materialized_model_1"
    assert matches[0]["match_score"] == "2 out of 3"
    assert ("a", "a") in matches[0]["matching_columns"]
    assert ("b", "b") in matches[0]["matching_columns"]
