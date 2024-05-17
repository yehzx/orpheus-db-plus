import pytest
from collections import OrderedDict
from dataclasses import dataclass
import csv
from orpheusplus.version_data import VersionData, HEAD_SUFFIX
from orpheusplus.cli import run

TEST_TABLE_NAME = "_test_table"
@dataclass
class Args:
    input: str
    output: str | None = None
    file = None

@pytest.fixture(scope="session")
def data_path():
    yield ["./tests/test_data/data_1.csv", "./tests/test_data/data_2.csv"]


@pytest.fixture(scope="session")
def expected_data():
    data_1 = [['employee_id', 'name', 'age', 'salary'],
              ['101', 'a', '30', '10340'],
              ['102', 'b', '18', '4000'],
              ['103', 'c', '40', '20500']]
    data_2 = [['employee_id', 'name', 'age', 'salary'],
              ['104', 'd', '23', '7000'],
              ['105', 'e', '21', '7400'],
              ['106', 'f', '32', '10320']]
    yield [data_1, data_2]


@pytest.fixture(scope="function")
def table(cnx):
    version_data = VersionData(cnx)
    version_data.init_table(TEST_TABLE_NAME, "./tests/test_data/sample_schema.csv")
    yield version_data
    version_data.remove()


def _check_data(tempdir):
    input = f"SELECT * FROM {TEST_TABLE_NAME}{HEAD_SUFFIX}"
    args = Args(input=input, output=tempdir / "output.csv")
    run(args)


def _read_result(tempdir):
    with open(tempdir / "output.csv", newline="") as f:
        reader = csv.reader(f)
        data = list(reader)
    return data


def test_c_insert(data_path, table: VersionData, expected_data, tempdir):
    table.from_file("insert", data_path[0])
    _check_data(tempdir)
    data = _read_result(tempdir)
    assert data == expected_data[0], f"result: {data}\nexpected: {expected_data[0]}"


def test_parse_csv_structure():
    result = VersionData._parse_csv_structure("./examples/sample_schema.csv")
    expected = "employee_id int, age int, salary int"
    assert result == expected, f"result: {result}\nexpected: {expected}"

def test_parse_csv_data():
    result = VersionData._parse_csv_data("./examples/data_1.csv")
    expected = [['101', '30', '10340'],
                ['102', '18', '4000'],
                ['103', '40', '20500']]
        
    assert result == expected, f"result: {result}\nexpected: {expected}"


def test_match_column_order():
    table_cols = ["employee_id", "age", "salary"]
    cols = ["Salary", "Age", "Employee_id"]
    result = VersionData._match_column_order(table_cols, cols)
    expected = [2, 1, 0]
    assert result == expected, f"result: {result}\nexpected: {expected}"


def test_reorder_data():
    data = [[1, 2, 3], ["foo", "bar", "foobar"]]
    order = [2, 1, 0]
    result = VersionData._reorder_data(data, order)
    expected = [[3, 2, 1], ["foobar", "bar", "foo"]]
    assert result == expected, f"result: {result}\nexpected: {expected}"
