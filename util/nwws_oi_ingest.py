"""Ingest data from NWWS-OI."""

# Third Party
from twisted.internet import reactor
from twisted.names.srvconnect import SRVConnector
from twisted.words.protocols.jabber import client, xmlstream
from twisted.words.protocols.jabber.jid import JID
from twisted.words.xish import domish


class Client:
    def __init__(self, jid, secret):
        f = client.XMPPClientFactory(jid, secret)
        f.addBootstrap(xmlstream.STREAM_CONNECTED_EVENT, self.connected)
        f.addBootstrap(xmlstream.STREAM_AUTHD_EVENT, self.authd)
        connector = SRVConnector(
            reactor, "xmpp-client", jid.host, f, defaultPort=5222,
        )
        connector.connect()

    def rawDataIn(self, buf):
        print("RECV: %r" % buf)

    def rawDataOut(self, buf):
        print("SEND: %r" % buf)

    def connected(self, xs):
        print("Connected.")

        self.xmlstream = xs
        self.xmlstream.addObserver("/message", self.on_message)

        # Log all traffic
        # xs.rawDataInFn = self.rawDataIn
        # xs.rawDataOutFn = self.rawDataOut

    def authd(self, xs):
        """authedn..."""
        presence = domish.Element(("jabber:client", "presence"))
        presence["to"] = "nwws@conference.nwws-oi.weather.gov/daryl.herzmann"
        self.xmlstream.send(presence)

    def on_message(self, elem):
        """Callback."""
        if elem.hasAttribute("type") and elem["type"] == "groupchat":
            self.processMessageGC(elem)

    def processMessageGC(self, elem):
        """Got message."""
        if elem.x:
            unixtext = str(elem.x)  # Unsure if this is the best way?
            noaaport = "\001" + unixtext.replace("\n\n", "\r\r\n")
            # Ensure product ends with \n
            if noaaport[-1] != "\n":
                noaaport = noaaport + "\r\r\n"
            noaaport = noaaport + "\003"
            # TODO ldm sequence number is only 3 char
            print(f"{repr(noaaport[:27])} sz:{len(noaaport)}")


if __name__ == "__main__":
    Client(JID("..."), "...")
    reactor.run()
