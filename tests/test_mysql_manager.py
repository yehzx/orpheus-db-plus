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


def test_c_mysql_connection():
    user = UserManager()
    mydb = MySQLManager(**user.info)
