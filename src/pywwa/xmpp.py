"""XMPP/Jabber Client Interface and Support."""
# stdlib
import inspect
import os
import re

# Third Party
import treq
from pyiem.util import LOG, utc
from twisted.internet import reactor
from twisted.internet.defer import inlineCallbacks
from twisted.internet.task import LoopingCall
from twisted.web import client as webclient
from twisted.words.protocols.jabber import client as jclient
from twisted.words.protocols.jabber import jid, xmlstream
from twisted.words.xish import domish, xpath
from twisted.words.xish.xmlstream import STREAM_END_EVENT

# Local
import pywwa

# http://stackoverflow.com/questions/7016602
webclient._HTTP11ClientFactory.noisy = False  # skipcq: PYL-W0212
SETTINGS = pywwa.SETTINGS
# create a regular expression that matches any illegal XML character
# http://stackoverflow.com/questions/1707890
ILLEGAL_XML_CHARS_RE = re.compile(
    "[\x00-\x08\x0b\x0c\x0e-\x1F\uD800-\uDFFF\uFFFE\uFFFF]", re.UNICODE
)


def make_jabber_client(resource_prefix=None):
    """Generate a jabber client, please"""
    if pywwa.CTX["disable_xmpp"]:
        LOG.info("XMPP disabled via command line.")
        pywwa.JABBER = NOOPXMPP()
        return pywwa.JABBER

    if resource_prefix is None:
        # Inspect the calling stack to determine the script name that is
        # calling us, so we can use that as the resource prefix
        frames = inspect.stack()
        resource_prefix = os.path.basename(frames[-1].filename).rstrip(".py")

    myjid = jid.JID(
        f"{SETTINGS.get('pywwa_jabber_username', 'iembot_ingest')}@"
        f"{SETTINGS.get('pywwa_jabber_domain', 'localhost')}/"
        f"{resource_prefix}_{utc():%Y%m%d%H%M%S}"
    )
    factory = jclient.XMPPClientFactory(
        myjid, SETTINGS.get("pywwa_jabber_password", "secret")
    )

    pywwa.JABBER = JabberClient(myjid)

    factory.addBootstrap("//event/stream/authd", pywwa.JABBER.authd)
    factory.addBootstrap("//event/client/basicauth/invaliduser", debug)
    factory.addBootstrap("//event/client/basicauth/authfailed", debug)
    factory.addBootstrap("//event/stream/error", debug)
    factory.addBootstrap(xmlstream.STREAM_END_EVENT, pywwa.JABBER.disconnect)

    reactor.connectTCP(
        SETTINGS.get("pywwa_jabber_host", "localhost"),  # @UndefinedVariable
        5222,
        factory,
    )
    return pywwa.JABBER


def message_processor(stanza):
    """Process a message stanza"""
    body = xpath.queryForString("/message/body", stanza)
    LOG.info("Message from %s Body: %s", stanza["from"], body)
    if body is not None and body.lower().strip() == "shutdown":
        LOG.info("I got shutdown message, shutting down...")
        pywwa.shutdown()


def raw_data_in(data):
    """
    Debug method
    @param data string of what was received from the server
    """
    if isinstance(data, bytes):
        data = data.decode("utf-8", errors="ignore")
    LOG.info("RECV %s", data)


def debug(elem):
    """
    Debug method
    @param elem twisted.works.xish
    """
    LOG.info(elem.toXml().encode("utf-8"))


def raw_data_out(data):
    """
    Debug method
    @param data string of what data was sent
    """
    if isinstance(data, bytes):
        data = data.decode("utf-8", errors="ignore")
    if data == " ":
        return
    LOG.info("SEND %s", data)


class NOOPXMPP:
    """A no-operation Jabber client."""

    def keepalive(self):
        """Do Nothing."""

    def send_message(self, *args):
        """Do Nothing."""


class JabberClient:
    """
    I am an important class of a jabber client against the chat server,
    used by pretty much every ingestor as that is how messages are routed
    to nwsbot
    """

    def __init__(self, myjid):
        """
        Constructor

        @param jid twisted.words.jid object
        """
        self.myjid = myjid
        self.xmlstream = None
        self.authenticated = False
        self.routerjid = (
            f"{SETTINGS.get('bot.username', 'iembot')}@"
            f"{SETTINGS.get('pywwa_jabber_domain', 'localhost')}"
        )

    def authd(self, xstream):
        """
        Callbacked once authentication succeeds
        @param xs twisted.words.xish.xmlstream
        """
        LOG.info("Logged in as %s", self.myjid)
        self.authenticated = True

        self.xmlstream = xstream
        self.xmlstream.rawDataInFn = raw_data_in
        self.xmlstream.rawDataOutFn = raw_data_out

        # Process Messages
        self.xmlstream.addObserver("/message/body", message_processor)

        # Send initial presence
        presence = domish.Element(("jabber:client", "presence"))
        presence.addElement("status").addContent("Online")
        self.xmlstream.send(presence)

        # Whitespace ping to keep our connection happy
        lc = LoopingCall(self.keepalive)
        lc.start(60)
        self.xmlstream.addObserver(STREAM_END_EVENT, lambda _: lc.stop())

    def keepalive(self):
        """
        Send whitespace ping to the server every so often
        """
        self.xmlstream.send(" ")

    def disconnect(self, _xs):
        """
        Called when we are disconnected from the server, I guess
        """
        LOG.info("SETTING authenticated to false!")
        self.authenticated = False

    @inlineCallbacks
    def send_message(self, body, html, xtra):
        """
        Send a message to nwsbot.  This message should have
        @param body plain text variant
        @param html html version of the message
        @param xtra dictionary of stuff that tags along
        """
        if not self.authenticated:
            LOG.info("No Connection, Lets wait and try later...")
            reactor.callLater(
                3,
                self.send_message,
                body,
                html,
                xtra,  # @UndefinedVariable
            )
            return
        message = domish.Element(("jabber:client", "message"))
        message["to"] = self.routerjid
        message["type"] = "chat"

        body = ILLEGAL_XML_CHARS_RE.sub("", body)
        if html:
            html = ILLEGAL_XML_CHARS_RE.sub("", html)
        message.addElement("body", None, body)
        helem = message.addElement(
            "html", "http://jabber.org/protocol/xhtml-im"
        )
        belem = helem.addElement("body", "http://www.w3.org/1999/xhtml")
        belem.addRawXml(html or body)
        # channels is of most important
        xelem = message.addElement("x", "nwschat:nwsbot")
        for key in xtra.keys():
            if isinstance(xtra[key], list):
                xelem[key] = ",".join(xtra[key])
            else:
                xelem[key] = xtra[key]

        # If we have twitter_media, we want to attempt to get the content
        # cached before iembot asks for it a million times
        url = xtra.get("twitter_media")
        if url is not None:
            try:
                # https leaks memory twisted/treq/issues/380
                _r = yield treq.get(url.replace("https", "http"), timeout=120)
            except Exception as exp:
                LOG.info("twitter_media request for %s failed: %s", url, exp)
        reactor.callFromThread(self.xmlstream.send, message)
