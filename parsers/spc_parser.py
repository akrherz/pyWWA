"""SPC Geo Products Parser!"""
from syslog import LOG_LOCAL2

from twisted.python import syslog
from twisted.python import log
from twisted.internet import reactor
from pyldm import ldmbridge
from pyiem.nws.products.spcpts import parser
import common  # @UnresolvedImport

syslog.startLogging(prefix='pyWWA/spc_parser', facility=LOG_LOCAL2)
DBPOOL = common.get_database('postgis')
WAITFOR = 20
JABBER = common.make_jabber_client('spc_parser')


# LDM Ingestor
class MyProductIngestor(ldmbridge.LDMProductReceiver):
    """ I receive products from ldmbridge and process them 1 by 1 :) """

    def connectionLost(self, reason):
        """shutdown"""
        log.msg('connectionLost')
        log.err(reason)
        reactor.callLater(5, self.shutdown)  # @UndefinedVariable

    def shutdown(self):
        """shutdown"""
        log.msg("shutdown() is called")
        reactor.callWhenRunning(reactor.stop)  # @UndefinedVariable

    def process_data(self, data):
        """ Process the product """
        df = DBPOOL.runInteraction(real_parser, data)
        df.addErrback(common.email_error, data)


def real_parser(txn, buf):
    """Actually process"""
    spc = parser(buf)
    # spc.draw_outlooks()
    spc.compute_wfos(txn)
    spc.sql(txn)
    jmsgs = spc.get_jabbers("")
    for (txt, html, xtra) in jmsgs:
        JABBER.send_message(txt, html, xtra)
    log.msg("Sent %s messages for product %s" % (len(jmsgs),
                                                 spc.get_product_id()))


if __name__ == '__main__':
    ldmbridge.LDMProductFactory(MyProductIngestor())
    reactor.run()  # @UndefinedVariable
