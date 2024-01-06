"""Test the ldmbrige."""

import mock
from pywwa import ldmbridge


def test_lineReceived():
    """Test lineReceived."""
    proto = ldmbridge.LDMProductReceiver(dedup=True)
    proto.reactor = mock.Mock()
    proto.process_data = mock.Mock()
    proto.clean_cache()
    proto.filter_product("test\r\r\ntest\r\r\ntest")
