""" HML parser! """
from syslog import LOG_LOCAL2
from twisted.python import syslog
syslog.startLogging(prefix='pyWWA/hml_parser', facility=LOG_LOCAL2)

# Twisted Python imports
from twisted.internet import reactor
from twisted.python import log

# Standard Python modules
import datetime
import common
import os

from pyldm import ldmbridge
from pyiem.nws.products.hml import parser as hmlparser

DBPOOL = common.get_database("hads")


# LDM Ingestor
class MyProductIngestor(ldmbridge.LDMProductReceiver):
    """ I receive products from ldmbridge and process them 1 by 1 :) """

    def connectionLost(self, reason):
        log.msg('connectionLost')
        log.err(reason)
        reactor.callLater(5, self.shutdown)

    def shutdown(self):
        reactor.callWhenRunning(reactor.stop)

    def process_data(self, buf):
        """ Process the product """
        defer = DBPOOL.runInteraction(real_parser, buf)
        defer.addErrback(common.email_error, buf)
        defer.addErrback(log.err)


def real_parser(txn, buf):
    """
    I'm gonna do the heavy lifting here
    """
    prod = hmlparser(buf)
    prod.sql(txn)
    if len(prod.warnings) > 0:
        common.email_error("\n".join(prod.warnings), buf)


def shutdown(err):
    log.msg(err)
    reactor.stop()

if __name__ == '__main__':
    ldmbridge.LDMProductFactory(MyProductIngestor())

    reactor.run()
