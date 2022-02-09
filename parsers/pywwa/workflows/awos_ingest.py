"""Reuse code from metar_parser."""

# 3rd party
from pyiem.util import LOG, utc
from twisted.internet import reactor
from twisted.internet.endpoints import TCP4ServerEndpoint
from twisted.internet.protocol import Factory
from twisted.protocols.basic import LineReceiver
# local
from pywwa.workflows import metar_parser
from ..database import get_database, load_metar_stations
from .. import common

IEMDB = get_database("iem")
ASOSDB = get_database("asos")


# pylint: disable=abstract-method
class AWOSProtocol(LineReceiver):
    """Our line receiver."""

    def lineReceived(self, line: bytes):
        """Do what we need to do."""
        reactor.callLater(0, process_line, line.decode("ascii", "ignore"))


class AWOSFactory(Factory):
    """Our protocol factory."""

    def buildProtocol(self, addr):
        """Build our protocol."""
        return AWOSProtocol()


def process_line(line):
    """Process a line of METAR data!"""
    # create a faked noaaport text product
    text = (
        "000 \r\r\n"
        f"SAUS43 KISU {utc():%d%H%M}\r\r\n"
        "METAR \r\r\n"
        f"{line}\r\r\n"
    )
    metar_parser.real_processor(text)


def ready(_):
    """Do what we need to do."""
    endpoint = TCP4ServerEndpoint(reactor, 4000)
    endpoint.listen(AWOSFactory())
    metar_parser.cleandb()


def main():
    """Run once at startup"""
    common.main()
    df = IEMDB.runInteraction(load_metar_stations, metar_parser.NWSLI_PROVIDER)
    df.addCallback(ready)
    df.addErrback(LOG.error)
    reactor.run()
