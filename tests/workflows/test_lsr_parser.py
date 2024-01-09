"""Test the LSR parser."""

from pywwa.workflows import lsr


def test_pickling():
    """Test the lifecycle of pickling the LSRDB."""
    lsr.pickledb()
    lsr.cleandb()
    lsr.loaddb()
