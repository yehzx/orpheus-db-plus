import pytest

from orpheusplus.cli import parse_args
from orpheusplus.version_data import VersionData

@pytest.fixture(scope="module")
def version_table(cnx):
    table_name = "temp_test"
    yield table_name
    table = VersionData(cnx)
    table.load_table(table_name)
    table.delete()


@pytest.mark.skipif("config.getoption('-s') == 'fd'")
def test_mysql_connection_config():
    args = parse_args(["config"])
    args.func(args)
    assert True


@pytest.mark.skipif("not config.getoption('--connection')")
def test_init_table(version_table):
    args = parse_args(["init",
                       "-s", "./examples/sample_schema.csv",
                       "-n", version_table])
    args.func(args)
    assert True