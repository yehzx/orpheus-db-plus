import pytest
from orpheusplus.mysql_manager import MySQLManager
from orpheusplus.user_manager import UserManager

def pytest_addoption(parser):
    parser.addoption("--connection", action="store_true",
                     help="Run tests that require database connections")

@pytest.fixture(scope="session")    
def cnx():
    try:
        user = UserManager()
        mydb = MySQLManager(**user.info)
        yield mydb
        del mydb
    except:
        yield None