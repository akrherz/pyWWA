"""Test the parsers/watch_parser.py module."""

import pytest
from pywwa.testing import get_example_file
from pywwa.workflows import watch_parser


@pytest.mark.parametrize("database", ["postgis"])
def test_parser(cursor):
    """Send a valid watch product to the parser."""
    for fn in ["SAW.txt", "SEL.txt", "WWP.txt"]:
        prod = watch_parser.real_process(cursor, get_example_file(fn))
        assert prod.warnings == []
        assert watch_parser.QUEUE[169][fn[:3]] is not None
    watch_parser.process_queue()
    assert watch_parser.QUEUE == {}
