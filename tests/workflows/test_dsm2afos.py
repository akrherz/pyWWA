"""Test dsm2afos workflow."""

from pywwa.testing import get_example_file
from pywwa.workflows.dsm2afos import workflow


def test_workflow():
    """run workflow."""
    raw = get_example_file("DSM.txt")
    workflow(raw)
