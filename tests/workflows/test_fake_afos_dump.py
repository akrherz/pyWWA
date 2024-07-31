"""Test fake_afos_dump."""

# 3rd Party
import pytest

# Local
import pywwa
from pyiem.nws.products.pirep import Pirep
from pyiem.util import utc
from pywwa.testing import get_example_file
from pywwa.workflows import fake_afos_dump


def test_fake_pirep():
    """Test fake PIREP."""
    tp = Pirep(get_example_file("PIREP.txt"), utcnow=utc(2024, 7, 9, 0, 0))
    assert tp.afos is None
    fake_afos_dump.compute_afos(tp)
    assert tp.afos == "PIREP"


@pytest.mark.parametrize("database", ["afos"])
def test_processor(cursor):
    """Test basic parsing."""
    data = get_example_file("CWA.txt")
    pywwa.CTX["utcnow"] = utc(2011, 3, 3, 6, 16)
    tp = fake_afos_dump.really_process_data(cursor, data)
    assert tp.valid == utc(2011, 3, 3, 1, 5)


@pytest.mark.parametrize("database", ["afos"])
def test_fake_noxx(cursor):
    """WMO header faked."""
    data = get_example_file("NOXX.txt")
    pywwa.CTX["utcnow"] = utc(2024, 1, 2, 18, 51)
    tp = fake_afos_dump.really_process_data(cursor, data)
    assert tp.afos == "ADMWNO"
