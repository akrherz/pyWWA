"""Test the LSR parser."""

from pywwa.workflows import lsr_parser


def test_pickling():
    """Test the lifecycle of pickling the LSRDB."""
    lsr_parser.pickledb()
    lsr_parser.cleandb()
    lsr_parser.loaddb()
