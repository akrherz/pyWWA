"""Test the LSR parser."""

from pywwa.workflows import lsr


def test_pickling():
    """Test the lifecycle of pickling the LSRDB."""
    lsr.pickledb()
    lsr.loaddb()
    # This writes, so be careful
    lsr.cleandb()
