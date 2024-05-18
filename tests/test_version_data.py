from datetime import datetime

import pytest

from orpheusplus.version_data import VersionData


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
    data_both = [['employee_id', 'name', 'age', 'salary'],
                 ['101', 'a', '30', '10340'],
                 ['102', 'b', '18', '4000'],
                 ['103', 'c', '40', '20500'],
                 ['104', 'd', '23', '7000'],
                 ['105', 'e', '21', '7400'],
                 ['106', 'f', '32', '10320']]
    yield [data_1, data_2, data_both]


def test_c_insert(func, data_path, table, expected_data, tempdir):
    table.from_file("insert", data_path[0])
    func.check_head()
    data = func.read_result()
    assert data == expected_data[0], f"result: {data}\nexpected: {expected_data[0]}"


def test_c_insert_delete(func, data_path, table, expected_data, tempdir):
    table.from_file("insert", data_path[0])
    table.from_file("delete", data_path[0])
    func.check_head()
    data = func.read_result()
    assert data == [expected_data[0][0]], f"result: {data}\nexpected: {[expected_data[0][0]]}"


def test_c_insert_delete(func, data_path, table, expected_data, tempdir):
    table.from_file("insert", data_path[0])
    table.from_file("update", [data_path[0], data_path[1]])
    func.check_head()
    data = func.read_result()
    assert data == expected_data[1], f"result: {data}\nexpected: {expected_data[1]}"


def test_c_commit_raise_error_1(table):
    now = datetime.now()
    with pytest.raises(SystemExit):
        table.commit(msg="version_1", now=now)


def test_c_commit_raise_error_2(table, data_path):
    now = datetime.now()
    with pytest.raises(SystemExit):
        table.from_file("insert", data_path[0])
        table.from_file("delete", data_path[0])
        table.commit(msg="version_1", now=now)


def test_c_commit(func, table_with_data, expected_data):
    func.check_version_table(1)
    result = func.read_result()
    assert result == expected_data[0], f"result: {result}\nexpected: {expected_data[0]}"

    func.check_version_table(2)
    result = func.read_result()
    assert result == expected_data[2], f"result: {result}\nexpected: {expected_data[2]}"
    
    func.check_version_table(3)
    result = func.read_result()
    assert result == expected_data[0], f"result: {result}\nexpected: {expected_data[0]}"
