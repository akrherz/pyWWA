"""Test mcd workflow."""

import pytest
from pywwa.testing import get_example_file
from pywwa.workflows import mcd


@pytest.mark.parametrize("database", ["postgis"])
def test_simple(cursor):
    """Test MCD processing."""
    mcd.real_process(cursor, get_example_file("MCD.txt"))
