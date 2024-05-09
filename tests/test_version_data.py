from collections import OrderedDict

from orpheusplus.version_data import VersionData


def test_parse_csv_structure():
    result = VersionData._parse_csv_structure("./examples/sample_schema.csv")
    expected = "employee_id int, age int, salary int"
    assert result == expected, f"result: {result}\nexpected: {expected}"

def test_parse_csv_data():
    result = VersionData._parse_csv_data("./examples/data.csv")
    expected = [(None, '101', '30', '10340'),
        (None, '102', '18', '4000'),
        (None, '103', '40', '20500'),
        (None, '104', '23', '7000'),
        (None, '105', '21', '7400'),
        (None, '106', '32', '10320'),
        (None, '107', '41', '54020'),
        (None, '108', '22', '8000')]
        
    assert result == expected, f"result: {result}\nexpected: {expected}"
