import pytest
import sqlparse
from orpheusplus.query_parser import SQLParser
from orpheusplus.version_data import HEAD_SUFFIX, DATA_TABLE_SUFFIX
from orpheusplus.version_table import VERSION_TABLE_SUFFIX


def test_strip_whitespace():
    stmt = sqlparse.parse("SELECT   * FROM foo")
    result = SQLParser._strip_whitespace(stmt[0])
    result = [token.value for token in result]
    expected = sqlparse.parse("SELECT * FROM foo")[0]
    expected = [expected[i].value for i in [0, 2, 4, 6]]
    assert result == expected, f"result: {result}\nexpected: {expected}"


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
        ("SELECT * FROM VTABLE foo OF VERSION 1;", f"SELECT * FROM foo{DATA_TABLE_SUFFIX} WHERE rid IN (SELECT * FROM foo{VERSION_TABLE_SUFFIX} WHERE version = 1);"),
        # VTABLE and VERSION and WHERE
        ("SELECT * FROM VTABLE foo OF VERSION 1 WHERE bar = 'baz';", f"SELECT * FROM foo{DATA_TABLE_SUFFIX} WHERE rid IN (SELECT * FROM foo{VERSION_TABLE_SUFFIX} WHERE version = 1) AND bar = 'baz';"),
    ]
)
def test_handle_vtable(input, expected):
    stmt = sqlparse.parse(input)
    tokens = SQLParser._strip_whitespace(stmt[0])
    tokens = SQLParser._handle_keywords(tokens)
    result = SQLParser._rebuild_stmt(tokens)
    assert result == expected, f"result: {result}\nexpected: {expected}"

    
    
    