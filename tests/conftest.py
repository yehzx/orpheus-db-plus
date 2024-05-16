import pytest
from orpheusplus.mysql_manager import MySQLManager
from orpheusplus.user_manager import UserManager


def pytest_addoption(parser):
    parser.addoption("--connection", action="store_true",
                     help="Run tests that require database connections")


def pytest_collection_modifyitems(config, items):
    skip_tests_need_cnx = pytest.mark.skipif("not config.getoption('--connection')")

    for item in items:
        if item.name.startswith("test_c_"):
            item.add_marker(skip_tests_need_cnx)


@pytest.fixture(scope="session")    
def cnx():
    try:
        user = UserManager()
        mydb = MySQLManager(**user.info)
        yield mydb
        del mydb
    except:
        yield None