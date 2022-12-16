"""Test spammer."""

# Local
from pywwa.workflows import spammer
from pywwa.testing import get_example_file


def test_spammer():
    """Test that we can delineate PNS!"""
    res = spammer.real_process(get_example_file("PNS_damage.txt"))
    assert res is not None
    res = spammer.real_process(get_example_file("PNS.txt"))
    assert res is None
