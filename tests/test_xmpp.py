"""Test pywwa.xmpp"""

from pywwa import xmpp
from twisted.words.xish import xmlstream


def test_illegal_xml():
    """Test that we can remove illegal XML characters."""
    assert xmpp.ILLEGAL_XML_CHARS_RE.sub("", "\003hello") == "hello"


def test_send_message():
    """Test the sending of messages."""
    client = xmpp.JabberClient("root@localhost")
    client.authenticated = True
    client.xmlstream = xmlstream.XmlStream()
    client.send_message("hello", "hello", {"channels": ["XX", "YY"], "t": "x"})


def test_client():
    """Test that we can use the JabberClient."""
    client = xmpp.JabberClient("root@localhost")
    assert client.routerjid


def test_make_jabber_client():
    """Test that we can build jabber clients."""
    client = xmpp.make_jabber_client()
    assert client.myjid
