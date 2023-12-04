"""Test bufr_surface workflow."""

import pytest
import pywwa
from pyiem.util import utc
from pywwa.testing import get_example_filepath
from pywwa.workflows import bufr_surface


def processor(cursor, buffn) -> int:
    """Helper for testing."""
    with open(get_example_filepath(f"BUFR/{buffn}.bufr"), "rb") as fh:
        prod, meat = bufr_surface.ingest(fh.read())
    return bufr_surface.processor(cursor, prod, meat)


@pytest.mark.parametrize("database", ["iem"])
def test_vsby(cursor):
    """Ob has visibility."""
    assert processor(cursor, "ISMI60_SBBR") == 60


def test_bounds_check():
    """Test that we can handle bad bounds."""
    assert bufr_surface.bounds_check(10, 0, 3) is None
    assert bufr_surface.bounds_check(None, 0, 10) is None


def test_api():
    """Test API."""
    bufr_surface.ready(None)
    bufr_surface.ingest(b"")


@pytest.mark.parametrize("database", ["mesosite"])
def test_add_station(cursor):
    """Exercise the add station logic."""
    assert bufr_surface.add_station(cursor, "46_&_2", {}) is None
    data = {"lon": -99, "lat": 42}
    assert bufr_surface.add_station(cursor, "46_&_2", data) is None
    data["sname"] = "TEST"
    assert bufr_surface.add_station(cursor, "46_&_2", data) is not None


@pytest.mark.parametrize("database", ["iem"])
def test_unknown_source(cursor):
    """Test that invalid descriptor is handled."""
    with open(get_example_filepath("BUFR/ISIA14_CWAO.bufr"), "rb") as fh:
        payload = fh.read()
    prod, meat = bufr_surface.ingest(payload.replace(b"CWAO", b"XXXX"))
    with pytest.raises(ValueError):
        bufr_surface.processor(cursor, prod, meat)


@pytest.mark.parametrize("database", ["iem"])
def test_231206_assertion_error(cursor):
    """Test something found in the wild."""
    processor(cursor, "ISMA01_FNLU")


@pytest.mark.parametrize("database", ["iem"])
def test_231206_nullbytes(cursor):
    """Test something found in the wild."""
    processor(cursor, "ISMA01_FNLU_badid")


@pytest.mark.parametrize("database", ["iem"])
def test_invalid_descriptor(cursor):
    """Test that invalid descriptor is handled."""
    processor(cursor, "ISXD03_EDZW")


@pytest.mark.parametrize("database", ["iem"])
def test_badmonth(cursor):
    """Test that month of 13 is handled."""
    processor(cursor, "ISIA14_CWAO")


@pytest.mark.parametrize("database", ["iem"])
def test_nonascii(cursor):
    """Test that non-ascii characters are handled."""
    processor(cursor, "ISAB02_CWAO")


@pytest.mark.parametrize("database", ["iem"])
def test_nosid(cursor):
    """Test that having no station id is handled."""
    processor(cursor, "ISXT14_EGRR")


@pytest.mark.parametrize("database", ["iem"])
def test_notimestamp(cursor):
    """Test that we don't error when no timestamp is found."""
    processor(cursor, "ISND01_LEMM")


@pytest.mark.parametrize("database", ["mesosite"])
def test_load_xref(cursor):
    """Test loading the xref."""
    cursor.execute(
        "insert into stations(id, wigos, network, iemid) "
        "values ('SCSC', 'SCSC', %s, -1)",
        (bufr_surface.NETWORK,),
    )
    bufr_surface.load_xref(cursor)


@pytest.mark.parametrize("database", ["iem"])
def test_simple(cursor):
    """Test simple."""
    pywwa.CTX.utcnow = utc(2023, 12, 4, 2)
    processor(cursor, "IS_2023120401")
