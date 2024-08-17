"""test gini2gis"""

from unittest.mock import patch

from pyiem.nws import gini
from pyiem.util import utc

import pywwa
from pywwa.testing import get_example_filepath
from pywwa.workflows import gini2gis


def make_gini():
    """Generate a GINI file."""
    with open(get_example_filepath("TIGH05"), "rb") as fh:
        res = gini.GINIZFile(fh)
    return res


def test_process_input():
    """Test process_input by mocking sys.stdin"""
    with patch("sys.stdin.buffer.read") as mock_read:
        with open(get_example_filepath("TIGH05"), "rb") as fh:
            mock_read.return_value = fh.read()
        assert gini2gis.process_input() is not None


def test_workflow():
    """Test the workflow."""
    pywwa.CTX["utcnow"] = utc(2021, 9, 16, 18)
    gini2gis.process_input = make_gini
    gini2gis.workflow()
