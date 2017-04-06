""" SPC Geo Products Parser! """

# Twisted Python imports
from syslog import LOG_LOCAL2
from twisted.python import syslog
syslog.startLogging(prefix='pyWWA/spc_parser', facility=LOG_LOCAL2)
from twisted.python import log
from twisted.internet import reactor

# pyWWA stuff
from pyldm import ldmbridge
from pyiem.nws.products.spcpts import parser
import common

DBPOOL = common.get_database('postgis')
WAITFOR = 20


# LDM Ingestor
class MyProductIngestor(ldmbridge.LDMProductReceiver):
    """ I receive products from ldmbridge and process them 1 by 1 :) """

    def connectionLost(self, reason):
        log.msg('connectionLost')
        log.err(reason)
        reactor.callLater(5, self.shutdown)

    def shutdown(self):
        log.msg("shutdown() is called")
        reactor.callWhenRunning(reactor.stop)

    def process_data(self, buf):
        """ Process the product """
        df = DBPOOL.runInteraction(real_parser, buf)
        df.addErrback(common.email_error, buf)


def real_parser(txn, buf):
    spc = parser(buf)
    # spc.draw_outlooks()
    spc.compute_wfos(txn)
    spc.sql(txn)
    for (txt, html, xtra) in spc.get_jabbers(""):
        jabber.sendMessage(txt, html, xtra)


if __name__ == '__main__':
    jabber = common.make_jabber_client('spc_parser')

    ldm = ldmbridge.LDMProductFactory(MyProductIngestor())
    reactor.run()
