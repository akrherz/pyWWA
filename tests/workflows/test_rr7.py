"""Exercise the rr7 workflow."""

import pytest

from pywwa.testing import get_example_file
from pywwa.workflows import rr7


@pytest.mark.parametrize("database", ["afos"])
def test_real_process(cursor):
    """Can we process a real RR7 product?"""
    data = get_example_file("SHEF/RR7ZOB.txt")
    rr7.real_process(cursor, data)
