"""Test spammer."""
import base64

from pyiem.nws.product import TextProduct
from pywwa.testing import get_example_file

# Local
from pywwa.workflows import spammer


def test_tornado_multi():
    """Test the parsing of EF scale."""
    prod = TextProduct(get_example_file("PNS_damage_multi2.txt"))
    res = spammer.damage_survey_pns(prod)
    pos = 0
    # Ensure order
    msg = base64.b64decode(res[2].get_payload()).decode("utf-8")
    for ef in range(3):
        thispos = msg.find(f"EF-{ef}")
        assert thispos > pos
        pos = thispos


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
