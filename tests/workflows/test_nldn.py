"""Test the NLDN workflow."""

import warnings

import pytest

from pywwa.testing import get_example_filepath
from pywwa.workflows.nldn import process_data


def test_nldn_empty():
    """Exercise an empty NLDN file."""
    assert process_data(None, b"") is None


@pytest.mark.parametrize("database", ["nldn"])
def test_nldn_workflow(cursor):
    """Test the NLDN workflow only inserts data once, le sigh."""
    fn = get_example_filepath("nldn.bin")
    with open(fn, "rb") as fh:
        # NB, this is verbatim from IDD, which has the NLDN prefix
        data = fh.read()[4:]
    prod = process_data(cursor, data)
    assert len(prod.df.index) == 16
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", FutureWarning)
        cursor.execute(
            "select count(*) from nldn_all where valid = ANY(%s)",
            [prod.df["valid"].dt.to_pydatetime().tolist()],
        )
    assert cursor.fetchone()["count"] == 16
