"""Test nexrad3_attr."""
# 3rd Party
import pytest

# Local
from pywwa.workflows import nexrad3_attr
from pywwa.testing import get_example_file


@pytest.mark.parametrize("database", ["mesosite"])
def test_load_station_table(cursor):
    """Test loading of the station table."""
    nexrad3_attr.load_station_table(cursor)


@pytest.mark.parametrize("database", ["radar"])
def test_process(cursor):
    """Test the processing of a level III file."""
    ctx = nexrad3_attr.process(
        get_example_file("NCR_20121127_1413", justfp=True)
    )
    assert ctx["nexrad"] == "JAX"
    processed = nexrad3_attr.really_process(cursor, ctx)
    assert processed == 3
