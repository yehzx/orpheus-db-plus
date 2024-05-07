import pytest

from orpheusplus.exceptions import MySQLConnectionError
from orpheusplus.mysql_manager import MySQLManager


def test_mysql_connection_error():
    with pytest.raises(MySQLConnectionError):
        mydb = MySQLManager(host="localhost",
                            user="foo",
                            passwd="bar",
                            database="foobar")


@pytest.mark.skipif("not config.getoption('--change_state')")
def test_mysql_connection_database_not_exist():
    # TODO: Need to make sure the host, user, passwd are successful before testing this
    pass


@pytest.mark.skipif("not config.getoption('--change_state')")
def test_execute_mysql_queries():
    # TODO: Come up with serveral SQL queries to test this
    pass
