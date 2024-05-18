import csv
import tempfile
from dataclasses import dataclass
from pathlib import Path

import pytest
from datetime import datetime
from orpheusplus.cli import run
from orpheusplus.mysql_manager import MySQLManager
from orpheusplus.user_manager import UserManager
from orpheusplus.version_data import (DATA_TABLE_SUFFIX, HEAD_SUFFIX,
                                      VersionData)
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
        yield mydb
    except:
        yield None


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


@pytest.fixture(scope="session")
def tempdir():
    with tempfile.TemporaryDirectory(dir="./tests") as dir:
        global TEMP_DIR
        temp_dir = Path(dir)
        TEMP_DIR = temp_dir
        yield temp_dir
        del TEMP_DIR


@dataclass
class Args:
    input: str
    output: str | None = None
    file = None


class Utils():
    @staticmethod
    def check_version_table(version):
        input = f"SELECT * FROM VTABLE {TEST_TABLE_NAME} OF VERSION {version}"
        args = Args(input=input, output=TEMP_DIR / "output.csv")
        run(args)

    @staticmethod
    def check_head():
        input = f"SELECT * FROM {TEST_TABLE_NAME}{HEAD_SUFFIX}"
        args = Args(input=input, output=TEMP_DIR / "output.csv")
        run(args)

    @staticmethod
    def read_result():
        with open(TEMP_DIR / "output.csv", newline="") as f:
            reader = csv.reader(f)
            data = list(reader)
        return data


@pytest.fixture(scope="session")
def func():
    return Utils()
