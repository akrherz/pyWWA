"""Exercise the sps workflow."""

import pytest

from pywwa.testing import get_example_file
from pywwa.workflows import sps


@pytest.mark.parametrize("database", ["postgis"])
def test_real_process(cursor):
    """Can we process a real SPS product?"""
    data = get_example_file("SPS.txt")
    sps.real_process({}, cursor, data)
