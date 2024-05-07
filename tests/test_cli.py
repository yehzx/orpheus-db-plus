import pytest

from orpheusplus.cli import parse_args


@pytest.mark.skipif("config.getoption('-s') == 'fd'")
def test_mysql_connection_config():
    args = parse_args(["config"])
    args.func(args)
    assert True


@pytest.mark.skipif("not config.getoption('--change_state')")
def test_init_table():
    args = parse_args(["init", "-t", "./examples/data.csv",
                       "-s", "./examples/sample_schema.csv",
                       "-n", "test_table"])
    args.func(args)
    assert True