"""Test aviation."""
# Local
from pywwa.workflows import aviation
from pywwa.testing import get_example_file


def test_processor():
    """Test basic parsing."""
    data = get_example_file("SIGC.txt")
    aviation.process_data(data)
