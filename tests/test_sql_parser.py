import pytest
from src.sql_parser import SqlParser

@pytest.fixture
def parser():
    return SqlParser()

def test_extract_table_references(parser):
    sql = "SELECT * FROM table1 JOIN table2 ON table1.id = table2.id"
    assert set(parser.extract_table_references(sql)) == {'table1', 'table2'}

def test_parse_joins(parser):
    sql = "SELECT * FROM table1 LEFT JOIN table2 ON table1.id = table2.id"
    joins = parser.parse_joins(sql)
    assert len(joins) == 1
    assert joins[0] == ('LEFT JOIN', 'table2', 'table1.id = table2.id')

def test_extract_transformations(parser):
    sql = "SELECT UPPER(name) as upper_name, age + 1 as age_plus_one FROM users"
    transformations = parser.extract_transformations(sql)
    assert 'name' in transformations
    assert transformations['name'][0]['type'] == 'function'
    assert transformations['name'][0]['function'] == 'UPPER'
    assert 'age' in transformations
    assert transformations['age'][0]['type'] == 'operation'
    assert transformations['age'][0]['operator'] == 'ADD'

def test_analyze_column_lineage(parser):
    sql = "SELECT id, name, age FROM users WHERE id = other_table.user_id"
    lineage = parser.analyze_column_lineage(sql)
    assert 'id' in lineage
    assert 'other_table.user_id' in lineage['id']['sources']

def test_complex_transformations(parser):
    sql = "SELECT CONCAT(first_name, ' ', last_name) as full_name, DATEDIFF(YEAR, birth_date, GETDATE()) as age FROM users"
    transformations = parser.extract_transformations(sql)
    assert 'first_name' in transformations
    assert 'last_name' in transformations
    assert 'birth_date' in transformations
    assert any('CONCAT' in t.get('function', '') for t in transformations['first_name'])
    assert any('DATEDIFF' in t.get('function', '') or 'TS_OR_DS_DIFF' in t.get('function', '') for t in transformations['birth_date'])

def test_arithmetic_operations(parser):
    sql = "SELECT price * quantity as total FROM orders"
    transformations = parser.extract_transformations(sql)
    assert 'price' in transformations
    assert transformations['price'][0]['type'] == 'operation'
    assert transformations['price'][0]['operator'] == 'MUL'
    assert transformations['price'][0]['right_operand'] == 'quantity'

def test_nested_functions(parser):
    sql = "SELECT CONCAT(UPPER(first_name), ' ', LOWER(last_name)) AS full_name, DATEDIFF(YEAR, birth_date, GETDATE()) AS age FROM users"
    transformations = parser.extract_transformations(sql)
    
    assert 'first_name' in transformations
    assert 'last_name' in transformations
    assert 'birth_date' in transformations
    
    # Check that functions are correctly identified
    assert any('UPPER' in t.get('function', '') for t in transformations['first_name'])
    assert any('LOWER' in t.get('function', '') for t in transformations['last_name'])
    assert any('DATEDIFF' in t.get('function', '') for t in transformations['birth_date'])
