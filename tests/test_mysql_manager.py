import pytest

from orpheusplus.exceptions import MySQLConnectionError
from orpheusplus.mysql_manager import MySQLManager
from orpheusplus.user_manager import UserManager


def test_mysql_connection_error():
    with pytest.raises(MySQLConnectionError):
        mydb = MySQLManager(host="localhost",
                            user="foo",
                            passwd="bar",
                            database="foobar")


@pytest.mark.skipif("not config.getoption('--connection') or config.getoption('-s') == 'fd'")
def test_mysql_connection_database_not_exist():
    # Should ask user to create a database
    user = UserManager()
    mydb = MySQLManager(user=user.info["user"],
                        passwd=user.info["passwd"],
                        database="XXXXXX")


@pytest.mark.skipif("not config.getoption('--connection')")
def test_execute_mysql_queries():
    # TODO: Come up with serveral SQL queries to test this
    pass
