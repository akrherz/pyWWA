"""Test afos_dump."""
# 3rd Party
import pytest

# Local
from pywwa.workflows import afos_dump
from pywwa.testing import get_example_file, cursor


@pytest.mark.parametrize("cursor", ["afos"])
def test_processor(cursor):
    """Test basic parsing."""
    data = get_example_file("AFD.txt")
    with pytest.raises(Exception):
        afos_dump.real_parser(cursor, data)
