from collections import OrderedDict

from orpheusplus.version_data import VersionData


def test_parse_csv_structure():
    result = VersionData._parse_csv_structure("./examples/sample_schema.csv")
    expected = "employee_id int, age int, salary int"
    assert result == expected, f"result: {result}\nexpected: {expected}"

def test_parse_csv_data():
    table_structure = OrderedDict([("employee_id", "int"), ("age", "int"), ("salary", "int")])
    result = VersionData._parse_csv_data("./examples/data.csv", table_structure)
    expected = [('NULL', 'CAST(101 AS INT)', 'CAST(30 AS INT)', 'CAST(10340 AS INT)'),
        ('NULL', 'CAST(102 AS INT)', 'CAST(18 AS INT)', 'CAST(4000 AS INT)'),
        ('NULL', 'CAST(103 AS INT)', 'CAST(40 AS INT)', 'CAST(20500 AS INT)'),
        ('NULL', 'CAST(104 AS INT)', 'CAST(23 AS INT)', 'CAST(7000 AS INT)'),
        ('NULL', 'CAST(105 AS INT)', 'CAST(21 AS INT)', 'CAST(7400 AS INT)'),
        ('NULL', 'CAST(106 AS INT)', 'CAST(32 AS INT)', 'CAST(10320 AS INT)'),
        ('NULL', 'CAST(107 AS INT)', 'CAST(41 AS INT)', 'CAST(54020 AS INT)'),
        ('NULL', 'CAST(108 AS INT)', 'CAST(22 AS INT)', 'CAST(8000 AS INT)')]
        
    assert result == expected, f"result: {result}\nexpected: {expected}"
