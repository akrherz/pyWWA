"""Test pywwa.ldm"""

# Local
from pywwa.ldm import bridge


def test_bridge():
    """Test that we can build a bridge."""
    assert bridge(None) is not None
