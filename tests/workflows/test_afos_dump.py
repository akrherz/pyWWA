"""Test afos_dump."""
# 3rd Party
import mock
import pytest

# Local
import pywwa
from pyiem.util import utc
from pywwa.testing import get_example_file
from pywwa.workflows import afos_dump


@pytest.mark.parametrize("database", ["afos"])
def test_future_product(cursor):
    """Test exception for product from the future."""
    data = get_example_file("AFD.txt")
    pywwa.CTX["utcnow"] = utc(2015, 6, 8, 11, 56)
    with pytest.raises(Exception):
        afos_dump.real_parser(cursor, data)


@pytest.mark.parametrize("database", ["afos"])
def test_memcache_write_api(cursor):
    """Exercise, but does not actually run the write :("""
    data = get_example_file("AFD.txt")
    pywwa.CTX["utcnow"] = utc(2015, 6, 9, 11, 56)
    prod = afos_dump.real_parser(cursor, data)
    mc = mock.Mock()
    afos_dump.actually_write(mc, prod)


@pytest.mark.parametrize("database", ["afos"])
def test_processor(cursor):
    """Test basic parsing."""
    data = get_example_file("AFD.txt")
    # 0. Very latent product
    with pytest.raises(Exception):
        afos_dump.real_parser(cursor, data)
    pywwa.CTX["utcnow"] = utc(2015, 6, 9, 11, 56)
    # 1. straight through
    prod = afos_dump.real_parser(cursor, data)
    assert prod.afos == "AFDDMX"
    # 2. Corrupt the AFOS
    data = data.replace("AFDDMX", "AFDDMX123")
    with pytest.raises(Exception):
        afos_dump.real_parser(cursor, data)
    # 3. Corrupt the AFOS and WMO source
    data = data.replace("KDMX", "QQQQ")
    res = afos_dump.real_parser(cursor, data)
    assert res is None
    # 4. Have something in exclude from memcache
    data = data.replace("AFDDMX123", "RR1DMX")
    res = afos_dump.real_parser(cursor, data)
    assert res is None
    # 5. Test write_memcache
    afos_dump.write_memcache(prod)
    afos_dump.write_memcache(None)
    # 6. Replace on
    pywwa.CTX["replace"] = True
    afos_dump.real_parser(cursor, data)


def test_empty():
    """Test what happens when we pass an emptry string."""
    assert afos_dump.real_parser(None, "") is None


def test_process_data():
    """Test API for now, noop."""
    afos_dump.process_data("")
