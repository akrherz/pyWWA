"""Test alaska_marine workflow."""

import pytest
from pyiem.util import utc

import pywwa
from pywwa.testing import get_example_file
from pywwa.workflows import alaska_marine


@pytest.mark.parametrize("database", ["postgis"])
def test_workflow(cursor):
    """Test failure in the wild."""
    pywwa.CTX["utcnow"] = utc(2025, 1, 1, 12)
    alaska_marine.process_data(cursor, get_example_file("CWFAER_0.txt"))
    alaska_marine.process_data(cursor, get_example_file("CWFAER_1.txt"))
    alaska_marine.process_data(cursor, get_example_file("CWFAER_2.txt"))

    cursor.execute("select count(*) from warnings_2025 where wfo = 'AFC'")
    assert cursor.fetchone()["count"] == 24  # not really cross-checked
