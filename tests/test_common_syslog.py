"""test import failure via a mock."""

import sys

import pywwa


class ImportErrorWhenAccessed:
    def __getattr__(self, name):
        raise ImportError


def test_import_error():
    """Test that we can handle an import error."""
    pywwa.SETTINGS["__setup_syslog"] = False
    from pywwa.common import setup_syslog

    for name in ("twisted.python", "twisted.python.syslog"):
        del sys.modules[name]

    sys.modules["twisted.python"] = ImportErrorWhenAccessed()
    setup_syslog()
    del sys.modules["twisted.python"]
