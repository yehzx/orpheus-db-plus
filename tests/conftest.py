import pytest

def pytest_addoption(parser):
    parser.addoption("--change_state", action="store_true",
                     help="Run tests that change database states")
    