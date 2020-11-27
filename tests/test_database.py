"""Test Database Functionality."""

# Local
from pywwa.database import load_ugcs_nwsli


def test_database():
    """Test that we can call the API."""
    load_ugcs_nwsli({}, {})
