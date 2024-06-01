from orpheusplus.utils import *


def test_parse_csv_structure():
    result = parse_csv_structure("./examples/sample_schema.csv")
    expected = [["employee_id", "int"], ["age", "int"], ["salary", "int"]]
    assert result == expected, f"result: {result}\nexpected: {expected}"


def test_parse_csv_data():
    result = parse_csv_data("./examples/data_1.csv")
    expected = [['101', '30', '10340'],
                ['102', '18', '4000'],
                ['103', '40', '20500']]
        
    assert result == expected, f"result: {result}\nexpected: {expected}"


def test_match_column_order():
    table_cols = ["employee_id", "age", "salary"]
    cols = ["Salary", "Age", "Employee_id"]
    result = match_column_order(table_cols, cols)
    expected = [2, 1, 0]
    assert result == expected, f"result: {result}\nexpected: {expected}"


def test_reorder_data():
    data = [[1, 2, 3], ["foo", "bar", "foobar"]]
    order = [2, 1, 0]
    result = reorder_data(data, order)
    expected = [[3, 2, 1], ["foobar", "bar", "foo"]]
    assert result == expected, f"result: {result}\nexpected: {expected}"