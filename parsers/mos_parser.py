""" MOS Data Ingestor, why not? """

from twisted.internet import reactor
from pyldm import ldmbridge
from pyiem.nws.products.mos import parser
import common  # @UnresolvedImport

DBPOOL = common.get_database("mos")
MEMORY = {"ingested": 0}


class MyProductIngestor(ldmbridge.LDMProductReceiver):
    """Do what we do!"""

    def process_data(self, data):
        """
        Actual ingestor
        """
        try:
            real_process(data)
        except Exception as myexp:
            common.email_error(myexp, data)

    def connectionLost(self, reason):
        """
        Called when ldm closes the pipe
        """
        reactor.callLater(5, self.shutdown)

    def shutdown(self):
        """Shutdown"""
        print("Saved %s entries to the database" % (MEMORY["ingested"],))
        reactor.callWhenRunning(reactor.stop)


def got_data(res):
    """Callback from the database save"""
    MEMORY["ingested"] += res


def real_process(text):
    """ The real processor of the raw data, fun! """
    prod = parser(text)
    if not common.dbwrite_enabled():
        return
    df = DBPOOL.runInteraction(prod.sql)
    df.addCallback(got_data)
    df.addErrback(common.email_error, text)


if __name__ == "__main__":
    ldmbridge.LDMProductFactory(MyProductIngestor())
    reactor.run()
