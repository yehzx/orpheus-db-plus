import pytest

from orpheusplus.cli import parse_args


@pytest.mark.skipif("config.getoption('-s') == 'fd'")
def test_mysql_connection_config():
    args = parse_args(["config"])
    args.func(args)
    assert True

def test_init_table():
    args = parse_args(["init", "-t", "foo", "-s", "bar"])
    args.func(args)
    assert True