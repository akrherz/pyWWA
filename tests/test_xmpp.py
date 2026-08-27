"""Test pywwa.xmpp"""

from unittest import mock

import pytest
import pytest_twisted
from twisted.words.protocols.jabber import jid
from twisted.words.xish import domish, xmlstream
from twisted.words.xish.domish import Element

from pywwa import CTX, xmpp


@pytest.fixture(autouse=True)
def reactor():
    """Fixture"""
    my = mock.Mock()

    def callme(func, *args, **kwargs):
        """Call the function if it is callable."""
        print("callme", args)
        return func(*args, **kwargs)

    my.callFromThread = callme

    return my


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


@pytest_twisted.inlineCallbacks
def test_gh341_double_encode(reactor):
    """Test that messages already with html entities do not get doubled."""
    client = xmpp.JabberClient(reactor, jid.JID("root@l"), "s")
    client.authenticated = True
    client.xmlstream = mock.Mock()

    captured = []

    def localsend(message: Element):
        """Local send method to capture the output."""
        print("localsend", type(message))
        captured.append(message)
        print("wrote captured")

    client.xmlstream.send = localsend
    _ = yield client.send_message(
        "This is already &gt; OK &",
        "<strong>Likewise &lt;gt;</strong> This is already &amp; OK",
        {
            "channels": ["XX", "YY"],
        },
    )
    sent_message = captured[0]
    ans = (
        "<message xmlns='jabber:client' to='iembot@localhost' type='chat'>"
        "<body>This is already &gt; OK &amp;</body>"
        "<html xmlns='http://jabber.org/protocol/xhtml-im'>"
        "<body xmlns='http://www.w3.org/1999/xhtml'>"
        "<strong>Likewise &lt;gt;</strong> This is already &amp; OK</body>"
        "</html><x xmlns='nwschat:nwsbot' channels='XX,YY'/></message>"
    )
    assert sent_message.toXml() == ans


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
