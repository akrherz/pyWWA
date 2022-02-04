"""Test metar_parser."""
# 3rd Party
import pytest
from pyiem.util import utc

# Local
import pywwa
from pywwa.workflows import metar_parser
from pywwa.testing import get_example_file


def test_api():
    """Test that we can load things to ignore."""
    metar_parser.load_ignorelist()
    metar_parser.cleandb()
    metar_parser.ready(None)


@pytest.mark.parametrize("database", ["postgis"])
def test_processor(cursor):
    """Test basic parsing."""
    data = get_example_file("METAR.txt")
    pywwa.CTX.utcnow = utc(2011, 11, 25, 14)
    for mtr in metar_parser.process_data(data).metars:
        metar_parser.do_db(cursor, mtr)
