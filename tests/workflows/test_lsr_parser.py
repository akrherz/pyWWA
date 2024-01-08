"""Test the LSR parser."""

from pywwa.workflows import lsr


# TODO this is not safe
def disabled_test_pickling():
    """Test the lifecycle of pickling the LSRDB."""
    lsr.pickledb()
    lsr.cleandb()
    lsr.loaddb()
