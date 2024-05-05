import pytest
from orpheusplus.cli import parse_args

@pytest.mark.skipif("config.getoption('-s') == 'fd'")
def test_mysql_connection_config():
    args = parse_args(["config"])
    args.func()
    assert True