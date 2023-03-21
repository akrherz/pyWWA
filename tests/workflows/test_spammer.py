"""Test spammer."""

from pyiem.nws.product import TextProduct

# Local
from pywwa.workflows import spammer
from pywwa.testing import get_example_file


def test_tornado_multi():
    """Test the parsing of EF scale."""
    prod = TextProduct(get_example_file("PNS_damage_multi.txt"))
    res = spammer.damage_survey_pns(prod)
    assert "EF1" in res[0]


def test_tornado_damage():
    """Test the parsing of EF scale."""
    prod = TextProduct(get_example_file("PNS_damage.txt"))
    res = spammer.damage_survey_pns(prod)
    assert "EF2" in res[0]


def test_spammer():
    """Test that we can delineate PNS!"""
    res = spammer.real_process(get_example_file("PNS_damage.txt"))
    assert res is not None
    res = spammer.real_process(get_example_file("PNS.txt"))
    assert res is None
