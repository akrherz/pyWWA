"""Test cli_parser."""
# 3rd Party
import pytest

# Local
import pywwa
from pyiem.util import utc
from pywwa.testing import get_example_file
from pywwa.workflows import cli_parser


@pytest.mark.parametrize("database", ["iem"])
def test_processor(cursor):
    """Test basic parsing."""
    data = get_example_file("CLI.txt")
    pywwa.CTX.utcnow = utc(2015, 6, 9, 6, 51)
    prod = cli_parser.processor(cursor, data)
    assert prod.valid == pywwa.CTX.utcnow


@pytest.mark.parametrize("database", ["iem"])
def test_two_clis(cursor):
    """Test parsing ye infamous double CLI."""
    cursor.execute(
        "select max_tmpf from summary_2014 s JOIN stations t "
        "on (s.iemid = t.iemid) WHERE t.id in ('HOU', 'IAH') "
        "and day = '2014-11-30'"
    )
    data = get_example_file("CLIHOU.txt")
    prod = cli_parser.processor(cursor, data)
    assert len(prod.data) == 2


@pytest.mark.parametrize("database", ["iem"])
def test_bad_station(cursor):
    """Test what happens when we have an unknown station."""
    data = get_example_file("CLI.txt").replace("CLIFGF", "CLIXXX")
    pywwa.CTX.utcnow = utc(2015, 6, 9, 6, 51)
    prod = cli_parser.processor(cursor, data)
    assert prod is not None
