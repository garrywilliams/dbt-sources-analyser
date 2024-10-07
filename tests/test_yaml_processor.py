import pytest
from src.yml_processor import process_yaml_sources

@pytest.fixture
def mock_yaml_dir(tmpdir):
    yaml_content = """
version: 2
sources:
  - name: group_1
    tables:
      - name: source_table_1
        columns:
          - name: column_1
          - name: column_2
  - name: group_2
    tables:
      - name: source_table_2
        columns:
          - name: column_a
          - name: column_b
"""
    yaml_file = tmpdir.join("source.yml")
    yaml_file.write(yaml_content)
    return str(tmpdir)

def test_process_yaml_sources(mock_yaml_dir):
    sources = process_yaml_sources(mock_yaml_dir)

    assert 'source_table_1' in sources
    assert sources['source_table_1']['group'] == 'group_1'
    assert 'column_1' in sources['source_table_1']['columns']
    assert 'column_2' in sources['source_table_1']['columns']

    assert 'source_table_2' in sources
    assert sources['source_table_2']['group'] == 'group_2'
    assert 'column_a' in sources['source_table_2']['columns']
    assert 'column_b' in sources['source_table_2']['columns']
