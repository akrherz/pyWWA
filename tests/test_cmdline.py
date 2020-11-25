"""Test command line flag parsing."""

# Local
from pywwa.cmdline import parse_cmdline


def test_cmdline():
    """Test that we can parse things."""
    ctx = parse_cmdline(["pytest", "blah"])
    assert ctx
