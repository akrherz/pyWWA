"""test gini2gis"""

from pyiem.nws import gini
from pywwa.testing import get_example_filepath
from pywwa.workflows import gini2gis


def make_gini():
    """Generate a GINI file."""
    with open(get_example_filepath("TIGH05"), "rb") as fh:
        res = gini.GINIZFile(fh)
    return res


def test_workflow():
    """Test the workflow."""
    gini2gis.process_input = make_gini
    gini2gis.main()
