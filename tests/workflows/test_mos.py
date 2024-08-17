"""test mos workflow"""

import pytest

from pywwa.testing import get_example_file
from pywwa.workflows.mos import process_data


@pytest.mark.parametrize("database", ["mos"])
def test_workflow(cursor):
    """Test."""
    process_data(cursor, get_example_file("METNC1.txt"))
