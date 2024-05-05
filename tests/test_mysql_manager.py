from orpheusplus.mysql_manager import MySQLManager
import mysql.connector
import pytest


def test_mysql_connection_error():
    with pytest.raises(mysql.connector.Error):
        mydb = MySQLManager(host="localhost",
                            user="foo",
                            passwd="bar",
                            database="foobar")