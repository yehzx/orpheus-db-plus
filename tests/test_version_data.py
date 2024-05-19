from datetime import datetime

import pytest


@pytest.fixture(scope="session")
def data_path():
    yield {"1": "./tests/test_data/data_1.csv",
           "2": "./tests/test_data/data_2.csv"}


@pytest.fixture(scope="session")
def expected_data():
    data_1 = [['101', 'a', '30', '10340'],
              ['102', 'b', '18', '4000'],
              ['103', 'c', '40', '20500']]
    data_2 = [['104', 'd', '23', '7000'],
              ['105', 'e', '21', '7400'],
              ['106', 'f', '32', '10320']]
    data_3 = [['201','g', '30', '10340'],    
              ['202','h', '18', '4000'],
              ['203','i', '40', '20500']]
    data_4 = [['204','j', '50', '10400'],    
              ['205','k', '30', '24000'],
              ['206','l', '25', '12500']]
    headers = [['employee_id', 'name', 'age', 'salary']]
    merge_1 = headers
    merge_2 = headers + data_2
    merge_3 = headers + data_2[:-1]
    merge_4 = headers + data_3 + data_4 + data_2
    merge_5 = headers + data_3 + data_2
    no_conflict = headers + data_2 + data_4 + data_2
    yield {"data_1": data_1, "data_2": data_2, "data_3": data_3, "data_4": data_4,
           "merge_1": merge_1, "merge_2": merge_2, "merge_3": merge_3, "merge_4": merge_4,
           "merge_5": merge_5, "no_conflict": no_conflict, "headers": headers}


def test_c_insert(func, data_path, table, expected_data):
    table.from_file("insert", data_path['1'])
    func.check_head()
    data = func.read_result()
    expected = expected_data["headers"] + expected_data["data_1"]
    assert data == expected, f"result: {data}\nexpected: {expected}"


def test_c_insert_delete(func, data_path, table, expected_data):
    table.from_file("insert", data_path['1'])
    table.from_file("delete", data_path['1'])
    func.check_head()
    data = func.read_result()
    assert data == expected_data['headers'], f"result: {data}\nexpected: {expected_data['headers']}"


def test_c_insert_update(func, data_path, table, expected_data):
    table.from_file("insert", data_path['1'])
    table.from_file("update", [data_path['1'], data_path['2']])
    func.check_head()
    data = func.read_result()
    expected = expected_data["headers"] + expected_data["data_2"]
    assert data == expected, f"result: {data}\nexpected: {expected}"


def test_c_commit_raise_error_1(table):
    now = datetime.now()
    with pytest.raises(SystemExit):
        table.commit(msg="version_1", now=now)


def test_c_commit_raise_error_2(table, data_path):
    now = datetime.now()
    with pytest.raises(SystemExit):
        table.from_file("insert", data_path["1"])
        table.from_file("delete", data_path["1"])
        table.commit(msg="version_1", now=now)


def test_c_commit(func, table_with_data, expected_data):
    func.check_version_table(1)
    result = func.read_result()
    expected = expected_data["headers"] + expected_data["data_1"]
    assert result == expected, f"result: {result}\nexpected: {expected}"

    func.check_version_table(2)
    result = func.read_result()
    expected = expected_data["headers"] + expected_data["data_1"] + expected_data["data_2"]
    assert result == expected, f"result: {result}\nexpected: {expected}"
    
    func.check_version_table(3)
    result = func.read_result()
    expected = expected_data["headers"] + expected_data["data_1"]
    assert result == expected, f"result: {result}\nexpected: {expected}"


def test_c_merge_1(func, table_for_merge, expected_data):
    table_for_merge.merge(2, resolved_file="./tests/test_data/conflicts_1.csv")
    func.check_version_table(4)
    result = func.read_result()
    assert result == expected_data['merge_1'], f"result: {result}\nexpected: {expected_data['merge_1']}"
    

def test_c_merge_2(func, table_for_merge, expected_data):
    table_for_merge.merge(2, resolved_file="./tests/test_data/conflicts_2.csv")
    func.check_version_table(4)
    result = func.read_result()
    assert result == expected_data['merge_2'], f"result: {result}\nexpected: {expected_data['merge_2']}"


def test_c_merge_3(func, table_for_merge, expected_data):
    table_for_merge.merge(2, resolved_file="./tests/test_data/conflicts_3.csv")
    func.check_version_table(4)
    result = func.read_result()
    assert result == expected_data['merge_3'], f"result: {result}\nexpected: {expected_data['merge_3']}"


def test_c_merge_4(func, larger_table_for_merge, expected_data):
    larger_table_for_merge.merge(4, resolved_file="./tests/test_data/conflicts_l_1.csv")
    func.check_version_table(7)
    result = func.read_result()
    assert result == expected_data['merge_4'], f"result: {result}\nexpected: {expected_data['merge_4']}"

    
def test_c_merge_5(func, larger_table_for_merge, expected_data):
    larger_table_for_merge.merge(4, resolved_file="./tests/test_data/conflicts_l_2.csv")
    func.check_version_table(7)
    result = func.read_result()
    assert result == expected_data['merge_5'], f"result: {result}\nexpected: {expected_data['merge_5']}"


def test_c_merge_no_conflict(func, larger_table_for_merge, expected_data):
    larger_table_for_merge.merge(2)
    func.check_version_table(7)
    result = func.read_result()
    assert result == expected_data['no_conflict'], f"result: {result}\nexpected: {expected_data['no_conflict']}"