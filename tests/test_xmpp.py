"""Test pywwa.xmpp"""

import mock
import pywwa
from pywwa import xmpp
from twisted.words.protocols.jabber import jid
from twisted.words.xish import domish, xmlstream


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


def test_client_authd():
    """Test the authd method."""
    client = xmpp.JabberClient("root@localhost")
    xs = xmlstream.XmlStream()
    xs.transport = mock.Mock()
    xs.transport.write = mock.Mock()
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
    pywwa.CTX["disable_xmpp"] = True
    assert isinstance(xmpp.make_jabber_client(), xmpp.NOOPXMPP)
    pywwa.CTX["disable_xmpp"] = False


def test_illegal_xml():
    """Test that we can remove illegal XML characters."""
    assert xmpp.ILLEGAL_XML_CHARS_RE.sub("", "\003hello") == "hello"


def test_send_message():
    """Test the sending of messages."""
    client = xmpp.JabberClient("root@localhost")
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


def test_client():
    """Test that we can use the JabberClient."""
    client = xmpp.JabberClient("root@localhost")
    assert client.routerjid


def test_make_jabber_client():
    """Test that we can build jabber clients."""
    client = xmpp.make_jabber_client()
    assert client.myjid.resource is not None


def test_iq_ping():
    """Test the response to an IQ ping request."""
    client = xmpp.JabberClient("root@localhost")
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


def test_iq_version():
    """Test the response to IQ version request."""
    client = xmpp.JabberClient(jid.JID("root@localhost/meme"))
    xs = xmlstream.XmlStream()
    xs.transport = mock.Mock()
    xs.transport.write = mock.Mock()
    client.authd(xs)
    iq = domish.Element(("jabber:client", "iq"))
    iq["from"] = "iembot@localhost"
    iq["to"] = "root@localhost"
    iq["type"] = "get"
    iq["id"] = "123"
    iq.addElement("query", "jabber:iq:version")
    client.iq_processor(iq)
