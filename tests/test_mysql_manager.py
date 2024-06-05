import pytest

from orpheusplus.exceptions import MySQLError
from orpheusplus.mysql_manager import MySQLManager
from orpheusplus.user_manager import UserManager


def test_mysql_connection_error():
    with pytest.raises(MySQLError):
        mydb = MySQLManager(host="localhost",
                            user="foo",
                            passwd="bar",
                            database="foobar")


def test_c_mysql_connection():
    user = UserManager()
    mydb = MySQLManager(**user.info)


def test_c_mysql_table_not_exist(capfd):
    user = UserManager()
    mydb = MySQLManager(**user.info)
    with pytest.raises(MySQLError) as e:
        mydb.execute("SELECT * FROM vtable_not_exist_orpheusplus")
    # Remove database name from output
    result = e.value.msg.replace(f"{user.info['database']}.", "").strip()
    expected = "Version Table `vtable_not_exist` doesn't exist."
    assert result == expected, f"result: {result}\nexpected: {expected}"

    with pytest.raises(MySQLError) as e:
        mydb.execute("SELECT * FROM table_not_exist")
    result = e.value.msg.replace(f"{user.info['database']}.", "").strip()
    expected = "Table `table_not_exist` doesn't exist."
    assert result == expected, f"result: {result}\nexpected: {expected}"
