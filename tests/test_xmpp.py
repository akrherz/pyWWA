"""Test pywwa.xmpp"""

# Local
from pywwa import xmpp


def test_client():
    """Test that we can use the JabberClient."""
    client = xmpp.JabberClient("root@localhost")
    assert client.routerjid


def test_make_jabber_client():
    """Test that we can build jabber clients."""
    client = xmpp.make_jabber_client()
    assert client.myjid
