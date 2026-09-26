"""Exercise the split_mav workflow."""

import pytest

from pywwa.testing import get_example_file
from pywwa.workflows import split_mav


@pytest.mark.parametrize("database", ["afos"])
def test_real_process(cursor):
    """Can we process a real SPS product?"""
    data = get_example_file("NBE_202609250000.txt")
    # File has two products, but second has station ID we don't care about
    assert split_mav.real_process(cursor, data) == 1
