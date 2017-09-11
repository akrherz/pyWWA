""" FFG """
from syslog import LOG_LOCAL2

from twisted.python import syslog
from twisted.python import log
from twisted.internet import reactor
from pyldm import ldmbridge
from pyiem.nws.products.ffg import parser
import common  # @UnresolvedImport

syslog.startLogging(prefix='pyWWA/ffg_parser', facility=LOG_LOCAL2)

DBPOOL = common.get_database('postgis', cp_max=1)
WAITFOR = 20


# LDM Ingestor
class MyProductIngestor(ldmbridge.LDMProductReceiver):
    """ I receive products from ldmbridge and process them 1 by 1 :) """

    def connectionLost(self, reason):
        """Connection was lost"""
        log.msg('connectionLost')
        log.err(reason)
        reactor.callLater(5, shutdown)  # @UndefinedVariable

    def process_data(self, data):
        """Process the product"""
        df = DBPOOL.runInteraction(real_parser, data)
        df.addErrback(common.email_error, data)


def shutdown():
    """Go shutdown"""
    reactor.callWhenRunning(reactor.stop)  # @UndefinedVariable


def real_parser(txn, buf):
    """callback func"""
    ffg = parser(buf)
    if ffg.afos == 'FFGMPD':
        return
    ffg.sql(txn)
    if ffg.warnings:
        common.email_error("\n".join(ffg.warnings), buf)
    sz = 0 if ffg.data is None else len(ffg.data.index)
    log.msg("FFG found %s entries for product %s" % (sz,
                                                     ffg.get_product_id()))


def main():
    """Our main method"""
    _ = ldmbridge.LDMProductFactory(MyProductIngestor())
    reactor.run()  # @UndefinedVariable


if __name__ == '__main__':
    main()
