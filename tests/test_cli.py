import pytest
from orpheusplus.cli import parse_args

@pytest.mark.skipif("not config.getoption('-s')")
def test_config():
    args = parse_args(["config"])
    args.func()
    assert True