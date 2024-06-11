import pytest
import sqlparse
from orpheusplus.query_parser import SQLParser
from orpheusplus.version_data import HEAD_SUFFIX as head_suffix, DATA_TABLE_SUFFIX
from orpheusplus.version_table import VERSION_TABLE_SUFFIX
from orpheusplus.user_manager import UserManager

HEAD_SUFFIX = head_suffix + "_" + UserManager().info["user"]

def test_strip_whitespace():
    stmt = sqlparse.parse("SELECT   * FROM foo")
    result = SQLParser._strip_unwanted_tokens(stmt[0])
    result = [token.value for token in result]
    expected = sqlparse.parse("SELECT * FROM foo")[0]
    expected = [expected[i].value for i in [0, 2, 4, 6]]
    assert result == expected, f"result: {result}\nexpected: {expected}"


@pytest.mark.parametrize("input",
    [
        "SELECT * FROM VTABLE foo VERSION 1",
        "SELECT * FROM VTABLE foo VERSION OF 1"
        "INSERT * INTO VTABLE foo",
        "INSERT INTO VTABLE foo OF VERSION 1",
        "INSERT INTO VTABLE foo, bar VALUES (1, 2, 3)",
        "DELETE VTABLE foo",
        "DELETE FROM VTABLE foo, bar",
        "DELETE VTABLE foo OF VERSION 1",
        "UPDATE VTABLE foo",
        "UPDATE VTABLE foo OF VERSION 1",
        "UPDATE VTABLE foo, bar SET baz = 1",
    ])
def test_raise_syntax_error(input):
    with pytest.raises(SyntaxError):
        stmt = sqlparse.parse(input)
        tokens = SQLParser._strip_unwanted_tokens(stmt[0])
        tokens = SQLParser._handle_keywords(tokens)


@pytest.mark.parametrize("input,expected",
    [   # Simple
        ("SELECT * FROM foo;", "SELECT * FROM foo;"),
        # Semicolon
        ("SELECT * FROM foo;", "SELECT * FROM foo;"),
        # WHERE
        ("SELECT * FROM foo WHERE bar = 'baz';", "SELECT * FROM foo WHERE bar = 'baz';"),
        # WHERE and AND
        ("SELECT * FROM foo WHERE bar = 'baz' AND baz = 'qux';", "SELECT * FROM foo WHERE bar = 'baz' AND baz = 'qux';"),
        # VTABLE
        ("SELECT * FROM VTABLE foo;", f"SELECT * FROM foo{HEAD_SUFFIX};"),
        # VTABLE and WHERE
        ("SELECT * FROM VTABLE foo WHERE bar = 'baz';", f"SELECT * FROM foo{HEAD_SUFFIX} WHERE bar = 'baz';"),
        # VTABLE and WHERE and AND
        ("SELECT * FROM VTABLE foo WHERE bar = 'baz' AND baz = 'qux';", f"SELECT * FROM foo{HEAD_SUFFIX} WHERE bar = 'baz' AND baz = 'qux';"),
        # VTABLE and VERSION
        ("SELECT * FROM VTABLE foo OF VERSION 1;", f"SELECT * FROM foo{DATA_TABLE_SUFFIX} WHERE rid IN (SELECT rid FROM foo{VERSION_TABLE_SUFFIX} WHERE version = 1);"),
        # VTABLE and VERSION and WHERE
        ("SELECT * FROM VTABLE foo OF VERSION 1 WHERE bar = 'baz';", f"SELECT * FROM foo{DATA_TABLE_SUFFIX} WHERE rid IN (SELECT rid FROM foo{VERSION_TABLE_SUFFIX} WHERE version = 1) AND bar = 'baz';"),
    ]
)
def test_handle_vtable(input, expected):
    stmt = sqlparse.parse(input)
    tokens = SQLParser._strip_unwanted_tokens(stmt[0])
    tokens = SQLParser._handle_keywords(tokens)
    result = SQLParser._rebuild_stmt(tokens)
    assert result == expected, f"result: {result}\nexpected: {expected}"


@pytest.mark.parametrize("input,expected",
    [
        ("SELECT * FROM foo WHERE bar = 1;", [4]),
        ("SELECT * FROM VTABLE foo WHERE bar IN (SELECT bar FROM baz WHERE qua = 1);", [5]),
    ]
)
def test_get_where(input, expected):
    stmt = sqlparse.parse(input)
    tokens = SQLParser._strip_unwanted_tokens(stmt[0])
    result = SQLParser._get_where(tokens)
    assert result == expected, f"result: {result}\nexpected: {expected}"    


@pytest.mark.parametrize("input,expected",
    [
        ("INSERT INTO VTABLE foo VALUES (1, 2);", {"table_name": "foo", "operation": "insert", "attributes": {"columns": None, "data": [["1", "2"]]}}),
        ("INSERT INTO VTABLE foo VALUES (1, 2), (3, 4);", {"table_name": "foo", "operation": "insert", "attributes": {"columns": None, "data": [["1", "2"], ["3", "4"]]}}),
        ("INSERT INTO VTABLE foo (bar, baz) VALUES (1, 2), (3, 4);", {"table_name": "foo", "operation": "insert", "attributes": {"columns": ["bar", "baz"], "data": [["1", "2"], ["3", "4"]]}}),
        ("INSERT INTO VTABLE foo (`bar`, `baz`) VALUES (1, 2), (3, 4);", {"table_name": "foo", "operation": "insert", "attributes": {"columns": ["bar", "baz"], "data": [["1", "2"], ["3", "4"]]}}),
        ("UPDATE VTABLE foo SET bar = 1, baz=2;", {"table_name": "foo", "operation": "update", "attributes": {"set": {"bar": "1", "baz": "2"}, "where": ""}}),
        ("UPDATE VTABLE foo SET bar = 1 WHERE baz = 2;", {"table_name": "foo", "operation": "update", "attributes": {"set": {"bar": "1"}, "where": "WHERE baz = 2;"}}),
        ("DELETE FROM VTABLE foo;", {"table_name": "foo", "operation": "delete", "attributes": {"where": ""}}),
        ("DELETE FROM VTABLE foo WHERE bar = 1;", {"table_name": "foo", "operation": "delete", "attributes": {"where": "WHERE bar = 1;"}}),
    ]
)
def test_parse_for_versiondata(input, expected):
    stmt = sqlparse.parse(input)
    tokens = SQLParser._strip_unwanted_tokens(stmt[0])
    result = SQLParser._parse_for_versiondata(tokens)
    assert result == expected, f"result: {result}\nexpected: {expected}"
