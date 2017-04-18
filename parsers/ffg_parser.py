""" FFG """

# Twisted Python imports
from syslog import LOG_LOCAL2
from twisted.python import syslog
syslog.startLogging(prefix='pyWWA/ffg_parser', facility=LOG_LOCAL2)
from twisted.python import log
from twisted.internet import reactor

# pyWWA stuff
from pyldm import ldmbridge
from pyiem.nws.products.ffg import parser
import common

DBPOOL = common.get_database('postgis', cp_max=1)
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
    ffg = parser(buf)
    if ffg.afos == 'FFGMPD':
        return
    ffg.sql(txn)
    if len(ffg.warnings) > 0:
        common.email_error("\n".join(ffg.warnings), buf)
    sz = 0 if ffg.data is None else len(ffg.data.index)
    log.msg("FFG found %s entries for product %s" % (sz,
                                                     ffg.get_product_id()))


if __name__ == '__main__':
    ldm = ldmbridge.LDMProductFactory(MyProductIngestor())
    reactor.run()
