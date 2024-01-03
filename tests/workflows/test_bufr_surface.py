"""Test bufr_surface workflow."""
import os

import pytest
from pyiem.database import get_dbconnc
from pywwa.testing import get_example_filepath
from pywwa.workflows import bufr_surface


def sync_workflow(cursor, buffn) -> int:
    """Run twice through workflow, eh"""
    mconn, mcursor = get_dbconnc("mesosite")
    bufr_surface.load_xref(mcursor)
    with open(get_example_filepath(f"BUFR/{buffn}"), "rb") as fh:
        prod, datalists = bufr_surface.workflow(fh.read(), cursor, mcursor)
    mconn.close()
    obs = []
    for datalist in datalists:
        obs.append(bufr_surface.datalist2iemob_data(datalist, prod.source))
    return prod, obs


def generate_testfiles():
    """Generate a list of test files in the examples/BUFR directory."""
    for filename in os.listdir(get_example_filepath("BUFR")):
        if not filename.endswith(".bufr"):
            continue
        yield filename


def test_phour():
    """Test that we get a message with a phour set."""
    prod, obs = sync_workflow(None, "ISAI01_SBBR.bufr")
    assert prod.source == "SBBR"
    assert abs(obs[182]["phour"] - 0.0079) < 0.0001


@pytest.mark.parametrize("database", ["iem"])
def test_240103_bad_date(cursor):
    """Test failure in the wild."""
    sync_workflow(cursor, "ISCI01_SABM.bufr")


@pytest.mark.parametrize("database", ["iem"])
def test_231208_null_bytes(cursor):
    """Test failure in the wild."""
    sync_workflow(cursor, "ISMA01_FNLU_badid2.bufr")


@pytest.mark.parametrize("database", ["iem"])
@pytest.mark.parametrize("buffn", generate_testfiles())
def test_bufr_files_in_examples(cursor, buffn):
    """Parse all our examples."""
    sync_workflow(cursor, buffn)


def test_bounds_check():
    """Test that we can handle bad bounds."""
    assert bufr_surface.bounds_check(10, 0, 3) is None
    assert bufr_surface.bounds_check(None, 0, 10) is None


def test_api():
    """Test API."""
    bufr_surface.ready(None)
    bufr_surface.workflow(b"")


@pytest.mark.parametrize("database", ["iem"])
def test_unknown_source(cursor):
    """Test that invalid descriptor is handled."""
    with open(get_example_filepath("BUFR/ISIA14_CWAO.bufr"), "rb") as fh:
        payload = fh.read()
    bufr_surface.workflow(payload.replace(b"CWAO", b"XXXX"))


@pytest.mark.parametrize("database", ["mesosite"])
def test_load_xref(cursor):
    """Test loading the xref."""
    cursor.execute(
        "insert into stations(id, wigos, network, iemid) "
        "values ('SCSC', 'SCSC', %s, -1)",
        (bufr_surface.NETWORK,),
    )
    bufr_surface.load_xref(cursor)
