"""Test nexrad3_attr."""
# 3rd Party
import pytest
from pywwa.testing import get_example_filepath

# Local
from pywwa.workflows import nexrad3_attr


@pytest.mark.parametrize("database", ["mesosite"])
def test_load_station_table(cursor):
    """Test loading of the station table."""
    nexrad3_attr.load_station_table(cursor)


@pytest.mark.parametrize("database", ["radar"])
def test_process(cursor):
    """Test the processing of a level III file."""
    with open(get_example_filepath("NCR_20121127_1413"), 'rb') as fh:
        ctx = nexrad3_attr.process(fh)
    assert ctx["nexrad"] == "JAX"
    processed = nexrad3_attr.really_process(cursor, ctx)
    assert processed == 3


@pytest.mark.parametrize("database", ["radar"])
def test_210910_badvil(cursor):
    """Test that a missing VIL does not cause issues."""
    with open(get_example_filepath("NCR_20210911_0023"), 'rb') as fh:
        ctx = nexrad3_attr.process(fh)
    assert ctx["nexrad"] == "LAS"
    processed = nexrad3_attr.really_process(cursor, ctx)
    assert processed == 2
