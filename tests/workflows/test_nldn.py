"""Test the NLDN workflow."""

import pytest

from pywwa.testing import get_example_filepath
from pywwa.workflows.nldn import process_data


def test_nldn_empty():
    """Exercise an empty NLDN file."""
    assert process_data(None, "") is None


@pytest.mark.parametrize("database", ["nldn"])
def test_nldn_workflow(cursor):
    """Test the NLDN workflow only inserts data once, le sigh."""
    fn = get_example_filepath("nldn")
    with open(fn) as fh:
        # NB, this is verbatim from IDD, which has the NLDN prefix
        data = fh.read()
    for line in data.splitlines():
        process_data(cursor, line)
    cursor.execute(
        "select count(*) from nldn2025_07 where valid > '2025-07-23' and "
        "valid < '2025-07-24'",
    )
    assert cursor.fetchone()["count"] == 1
