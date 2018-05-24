""" HML parser! """
from syslog import LOG_LOCAL2

from twisted.internet import reactor
from twisted.python import log
from twisted.python import syslog
from pyldm import ldmbridge
from pyiem.nws.products.hml import parser as hmlparser

import common  # @UnresolvedImport

syslog.startLogging(prefix='pyWWA/hml_parser', facility=LOG_LOCAL2)
DBPOOL = common.get_database("hads")


# LDM Ingestor
class MyProductIngestor(ldmbridge.LDMProductReceiver):
    """ I receive products from ldmbridge and process them 1 by 1 :) """

    def connectionLost(self, reason):
        """Connection was lost"""
        log.msg('connectionLost')
        log.err(reason)
        reactor.callLater(5, reactor.stop)  # @UndefinedVariable

    def process_data(self, data):
        """ Process the product """
        defer = DBPOOL.runInteraction(real_parser, data)
        defer.addErrback(common.email_error, data)
        defer.addErrback(log.err)


def real_parser(txn, buf):
    """I'm gonna do the heavy lifting here"""
    prod = hmlparser(buf)
    prod.sql(txn)
    if prod.warnings:
        common.email_error("\n".join(prod.warnings), buf)


if __name__ == '__main__':
    ldmbridge.LDMProductFactory(MyProductIngestor())

    reactor.run()  # @UndefinedVariable
