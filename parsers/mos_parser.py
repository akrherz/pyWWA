""" MOS Data Ingestor, why not? """
from __future__ import print_function
from syslog import LOG_LOCAL2


import common  # @UnresolvedImport
from twisted.internet import reactor
from twisted.python import syslog
from pyldm import ldmbridge
from pyiem.nws.products.mos import parser
syslog.startLogging(prefix='pyWWA/mos_parser', facility=LOG_LOCAL2)


DBPOOL = common.get_database('mos')
MEMORY = {'ingested': 0}


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
        print("Saved %s entries to the database" % (MEMORY['ingested'], ))
        reactor.callWhenRunning(reactor.stop)


def got_data(res):
    """Callback from the database save"""
    MEMORY['ingested'] += res


def real_process(text):
    """ The real processor of the raw data, fun! """
    prod = parser(text)
    df = DBPOOL.runInteraction(prod.sql)
    df.addCallback(got_data)
    df.addErrback(common.email_error, text)


if __name__ == '__main__':
    ldmbridge.LDMProductFactory(MyProductIngestor())
    reactor.run()
