"""test import failure via a mock."""
import sys


class ImportErrorWhenAccessed:
    def __getattr__(self, name):
        raise ImportError


def test_import_error():
    """Test that we can handle an import error."""
    assert "twisted.python.syslog" not in sys.modules
    from pywwa.common import setup_syslog

    sys.modules["twisted.python"] = ImportErrorWhenAccessed()
    setup_syslog()
