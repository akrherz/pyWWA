"""Test pywwa.xmpp"""

from unittest import mock

import pytest
from twisted.words.protocols.jabber import jid
from twisted.words.xish import domish, xmlstream

from pywwa import CTX, xmpp


@pytest.fixture(autouse=True)
def reactor():
    """Fixture"""
    return mock.Mock()


def test_raw_data_in():
    """Test the raw data in method."""
    xmpp.raw_data_in(b"hello")


def test_debug():
    """Test the debug method."""
    elem = domish.Element(("jabber:client", "message"))
    xmpp.debug(elem)
    xmpp.debug("hello")


def test_raw_data_out():
    """Test the raw data out method."""
    xmpp.raw_data_out(b"hello")
    xmpp.raw_data_out(" ")


def test_client_authd(reactor):
    """Test the authd method."""
    client = xmpp.JabberClient(reactor, jid.JID("root@localhost"), "secret")
    xs = xmlstream.XmlStream()
    xs.transport = mock.Mock()
    xs.transport.write = mock.Mock()
    client.connected(xs)
    client.authd(xs)


def test_message_processor():
    """Test the message processor."""
    elem = domish.Element(("jabber:client", "message"))
    elem["from"] = "root@localhost"
    elem["to"] = "iembot@localhost"
    elem.addElement("body", content="shutdown")
    xmpp.message_processor(elem)


def test_disabled_xmpp():
    """Test that it works when XMPP is disabled."""
    CTX["disable_xmpp"] = True
    assert isinstance(xmpp.make_jabber_client(), xmpp.NOOPXMPP)
    CTX["disable_xmpp"] = False


def test_illegal_xml():
    """Test that we can remove illegal XML characters."""
    assert xmpp.ILLEGAL_XML_CHARS_RE.sub("", "\003hello") == "hello"


def test_send_message(reactor):
    """Test the sending of messages."""
    client = xmpp.JabberClient(reactor, jid.JID("root@localhost"), "secret")
    client.authenticated = True
    client.xmlstream = xmlstream.XmlStream()
    client.send_message(
        "hello",
        "hello",
        {
            "channels": ["XX", "YY"],
            "t": "x",
            "twitter_media": "http://thiswillfail.lazz/bogus",
        },
    )
    client.disconnect(None)


def test_client(reactor):
    """Test that we can use the JabberClient."""
    client = xmpp.JabberClient(reactor, jid.JID("root@localhost"), "secret")
    assert client.routerjid


def test_make_jabber_client():
    """Test that we can build jabber clients."""
    client = xmpp.make_jabber_client()
    assert client.myjid.resource is not None


def test_iq_ping(reactor):
    """Test the response to an IQ ping request."""
    client = xmpp.JabberClient(reactor, jid.JID("root@localhost"), "secret")
    client.xmlstream = xmlstream.XmlStream()
    client.xmlstream.transport = mock.Mock()
    client.xmlstream.transport.write = mock.Mock()
    ping = domish.Element(("jabber:client", "iq"))
    ping["from"] = "iembot@localhost"
    ping["to"] = "root@localhost"
    ping["type"] = "get"
    ping["id"] = "123"
    ping.addElement("ping", "urn:xmpp:ping")
    client.iq_processor(ping)


def test_iq_version(reactor):
    """Test the response to IQ version request."""
    client = xmpp.JabberClient(reactor, jid.JID("root@localhost/me"), "secret")
    xs = xmlstream.XmlStream()
    xs.transport = mock.Mock()
    xs.transport.write = mock.Mock()
    client.connected(xs)
    client.authd(xs)
    iq = domish.Element(("jabber:client", "iq"))
    iq["from"] = "iembot@localhost"
    iq["to"] = "root@localhost"
    iq["type"] = "get"
    iq["id"] = "123"
    iq.addElement("query", "jabber:iq:version")
    client.iq_processor(iq)
