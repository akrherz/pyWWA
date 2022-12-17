"""Ingest data from NWWS-OI."""
# stdlb

# Third Party
from pyiem.util import get_properties, utc
from twisted.internet import reactor
from twisted.internet.task import LoopingCall
from twisted.names.srvconnect import SRVConnector
from twisted.words.protocols.jabber import client, error, xmlstream
from twisted.words.protocols.jabber.jid import JID
from twisted.words.xish import domish
from twisted.words.xish.xmlstream import STREAM_END_EVENT


class Client:
    """A Jabber Client."""

    def __init__(self, jid, secret):
        """Constructor."""
        self.outstanding_pings = []
        self.xmlstream = None
        f = client.XMPPClientFactory(jid, secret)
        f.addBootstrap(xmlstream.STREAM_CONNECTED_EVENT, self.connected)
        f.addBootstrap(xmlstream.STREAM_AUTHD_EVENT, self.authd)
        connector = SRVConnector(
            reactor,
            "xmpp-client",
            jid.host,
            f,
            defaultPort=5222,
        )
        connector.connect()

    def connected(self, xs):
        """Connected."""
        print("Connected.")
        self.xmlstream = xs
        self.xmlstream.addObserver("/message", self.on_message)
        self.xmlstream.addObserver("/iq", self.on_iq)

    def authd(self, _xs):
        """authedn..."""
        self.outstanding_pings = []
        presence = domish.Element(("jabber:client", "presence"))
        presence["to"] = "nwws@conference.nwws-oi.weather.gov/daryl.herzmann"
        self.xmlstream.send(presence)
        lc = LoopingCall(self.housekeeping)
        lc.start(60)
        self.xmlstream.addObserver(STREAM_END_EVENT, lambda _x: lc.stop)

    def housekeeping(self):
        """
        This gets exec'd every minute to keep up after ourselves
        1. XMPP Server Ping
        2. Update presence
        """
        if self.outstanding_pings:
            print(f"Currently unresponded pings: {self.outstanding_pings}")
        if len(self.outstanding_pings) > 5:
            self.outstanding_pings = []
            if self.xmlstream is not None:
                # Unsure of the proper code that a client should generate
                exc = error.StreamError("gone")
                self.xmlstream.sendStreamError(exc)
            return
        if self.xmlstream is None:
            print("xmlstream is None, not sending ping")
            return
        utcnow = utc()
        ping = domish.Element((None, "iq"))
        ping["to"] = "nwws-oi.weather.gov"
        ping["type"] = "get"
        pingid = f"{utcnow:%Y%m%d%H%M}"
        ping["id"] = pingid
        ping.addChild(domish.Element(("urn:xmpp:ping", "ping")))
        self.outstanding_pings.append(pingid)
        self.xmlstream.send(ping)

    def on_iq(self, elem: domish.Element):
        """Process IQ message."""
        typ = elem.getAttribute("type")
        # A response is being requested of us.
        if typ == "get" and elem.firstChildElement().name == "ping":
            # Respond to a ping request.
            pong = domish.Element((None, "iq"))
            pong["type"] = "result"
            pong["to"] = elem["from"]
            pong["from"] = elem["to"]
            pong["id"] = elem["id"]
            self.xmlstream.send(pong)
        # We are getting a response to a request we sent, maybe.
        elif typ == "result":
            if elem.getAttribute("id") in self.outstanding_pings:
                self.outstanding_pings.remove(elem.getAttribute("id"))

    def on_message(self, elem):
        """Callback."""
        if elem.hasAttribute("type") and elem["type"] == "groupchat":
            self.processMessageGC(elem)

    def processMessageGC(self, elem):
        """Got message."""
        if not elem.x:
            print(f"Unknown elem? {elem.toXml()}")
            return
        unixtext = str(elem.x)  # Unsure if this is the best way?
        noaaport = "\001" + unixtext.replace("\n\n", "\r\r\n")
        # Ensure product ends with \n
        if noaaport[-1] != "\n":
            noaaport = noaaport + "\r\r\n"
        noaaport = noaaport + "\003"
        # Someday perhaps fix the ldm sequence number is only 3 char
        with open(f"/mesonet/tmp/nwwsoi/{utc():%Y%m%d%H}.txt", "ab") as fh:
            fh.write(noaaport.encode("utf-8"))


def main():
    """Go Main Go."""
    props = get_properties()
    Client(
        JID(f"{props['nwws-oi.username']}@nwws-oi.weather.gov"),
        props["nwws-oi.password"],
    )
    reactor.run()


if __name__ == "__main__":
    main()
