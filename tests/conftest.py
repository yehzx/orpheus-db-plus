import csv
import tempfile
from datetime import datetime
from pathlib import Path

import pytest

from orpheusplus.cli import run
from orpheusplus.mysql_manager import MySQLManager
from orpheusplus.user_manager import UserManager
from orpheusplus.version_data import DATA_TABLE_SUFFIX
from orpheusplus.version_data import HEAD_SUFFIX as head_suffix
from orpheusplus.version_data import VersionData
from orpheusplus.version_table import VERSION_TABLE_SUFFIX

TEST_TABLE_NAME = "_test_table"


def pytest_addoption(parser):
    parser.addoption("--connection", action="store_true",
                     help="Run tests that require database connections")


def pytest_collection_modifyitems(config, items):
    skip_tests_need_cnx = pytest.mark.skipif(
        "not config.getoption('--connection')")

    for item in items:
        if item.name.startswith("test_c_"):
            item.add_marker(skip_tests_need_cnx)


@pytest.fixture(scope="session")
def cnx():
    try:
        user = UserManager()
        mydb = MySQLManager(**user.info)
        global HEAD_SUFFIX
        HEAD_SUFFIX = head_suffix + "_" + user.info["user"]
        yield mydb
    except:
        yield None


@pytest.fixture(scope="session")
def tempdir():
    with tempfile.TemporaryDirectory(dir="./tests") as dir:
        temp_dir = Path(dir)
        yield temp_dir


def _drop_table_if_exists(cnx):
    cnx.execute(f"DROP TABLE IF EXISTS {TEST_TABLE_NAME}{DATA_TABLE_SUFFIX}")
    cnx.execute(f"DROP TABLE IF EXISTS {TEST_TABLE_NAME}{HEAD_SUFFIX}")
    cnx.execute(
        f"DROP TABLE IF EXISTS {TEST_TABLE_NAME}{VERSION_TABLE_SUFFIX}")


@pytest.fixture(scope="function")
def table(cnx):
    _drop_table_if_exists(cnx)
    version_data = VersionData(cnx)
    version_data.init_table(
        TEST_TABLE_NAME, "./tests/test_data/sample_schema.csv")
    yield version_data
    version_data.remove()


@pytest.fixture(scope="function")
def table_with_data(cnx):
    _drop_table_if_exists(cnx)
    version_data = VersionData(cnx)
    version_data.init_table(TEST_TABLE_NAME, "./tests/test_data/sample_schema.csv")
    now = datetime.now()
    version_data.from_file("insert", "./tests/test_data/data_1.csv")
    version_data.commit(msg="version_1", now=now)
    version_data.from_file("insert", "./tests/test_data/data_2.csv")
    version_data.commit(msg="version_2", now=now)
    version_data.from_file("delete", "./tests/test_data/data_2.csv")
    version_data.commit(msg="version_3", now=now)
    yield version_data
    version_data.remove()


@pytest.fixture(scope="function")
def table_for_merge(cnx):
    _drop_table_if_exists(cnx)
    version_data = VersionData(cnx)
    version_data.init_table(TEST_TABLE_NAME, "./tests/test_data/sample_schema.csv")
    now = datetime.now()
    version_data.from_file("insert", "./tests/test_data/data_1.csv")
    version_data.commit(msg="version_1", now=now)
    version_data.from_file("update", ["./tests/test_data/data_1.csv", "./tests/test_data/data_2.csv"])
    version_data.commit(msg="version_2", now=now)
    version_data.checkout(1)
    version_data.from_file("delete", "./tests/test_data/data_1.csv")
    version_data.commit(msg="version_3", now=now)
    yield version_data
    version_data.remove()    


@pytest.fixture(scope="function")
def larger_table_for_merge(cnx):
    _drop_table_if_exists(cnx)
    version_data = VersionData(cnx)
    version_data.init_table(TEST_TABLE_NAME, "./tests/test_data/sample_schema.csv")
    now = datetime.now()
    version_data.from_file("insert", "./tests/test_data/data_1.csv")
    version_data.commit(msg="version_1", now=now)
    version_data.from_file("insert", "./tests/test_data/data_2.csv")
    version_data.commit(msg="version_2", now=now)
    version_data.from_file("update", ["./tests/test_data/data_2.csv", "./tests/test_data/data_3.csv"])
    version_data.commit(msg="version_3", now=now)
    version_data.from_file("delete", "./tests/test_data/data_1.csv")
    version_data.commit(msg="version_4", now=now)
    version_data.checkout(1)
    version_data.from_file("update", ["./tests/test_data/data_1.csv", "./tests/test_data/data_4.csv"])
    version_data.commit(msg="version_5", now=now)
    version_data.from_file("insert", "./tests/test_data/data_2.csv")
    version_data.commit(msg="version_6", now=now)
    yield version_data
    version_data.remove()       


class Utils():
    args = dict(input=None, output=None, file=None, no_headers=False)

    @staticmethod
    def _set_tempdir(tempdir):
        Utils.tempdir = tempdir

    @staticmethod
    def check_version_table(version):
        input = f"SELECT * FROM VTABLE {TEST_TABLE_NAME} OF VERSION {version}"
        Utils.args.update(input=input, output=Utils.tempdir / "output.csv")
        run(Utils.args)

    @staticmethod
    def check_head():
        input = f"SELECT * FROM {TEST_TABLE_NAME}{HEAD_SUFFIX}"
        Utils.args.update(input=input, output=Utils.tempdir / "output.csv")
        run(Utils.args)

    @staticmethod
    def read_result():
        with open(Utils.tempdir / "output.csv", newline="") as f:
            reader = csv.reader(f)
            data = list(reader)
        return data


@pytest.fixture(scope="session")
def func(tempdir):
    utils = Utils()
    utils._set_tempdir(tempdir)
    yield utils 
