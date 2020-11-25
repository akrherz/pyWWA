"""Test pywwa.xmpp"""

# Local
from pywwa import xmpp


def test_client():
    """Test that we can use the JabberClient."""
    client = xmpp.JabberClient("root@localhost")
    assert client.routerjid
