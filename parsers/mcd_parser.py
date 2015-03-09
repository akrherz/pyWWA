"""
 Support SPC's MCD product
 Support WPC's FFG product
"""
from syslog import LOG_LOCAL2
from twisted.python import syslog
syslog.startLogging(prefix='pyWWA/mcd_parser', facility=LOG_LOCAL2)

from twisted.python import log
from twisted.internet import reactor

from pyiem.nws.products.mcd import parser as mcdparser
from pyldm import ldmbridge

import common

DBPOOL = common.get_database(common.config['databaserw']['postgis'],
                             cp_max=2)


# LDM Ingestor
class MyProductIngestor(ldmbridge.LDMProductReceiver):
    """ I receive products from ldmbridge and process them 1 by 1 :) """

    def connectionLost(self, reason):
        ''' Called when the STDIN connection is lost '''
        log.msg('connectionLost')
        log.err(reason)
        reactor.callLater(15, reactor.callWhenRunning, reactor.stop)

    def process_data(self, raw):
        ''' Process a chunk of data '''
        df = DBPOOL.runInteraction(real_process, raw)
        df.addErrback(common.email_error, raw)


def real_process(txn, raw):
    """"
    Actually process a single MCD
    """
    prod = mcdparser(raw)
    prod.find_cwsus(txn)

    j = prod.get_jabbers(common.settings.get('pywwa_product_url',
                                             'pywwa_product_url'))
    if len(j) == 1:
        JABBER.sendMessage(j[0][0], j[0][1], j[0][2])

    prod.database_save(txn)

ldmbridge.LDMProductFactory(MyProductIngestor())
JABBER = common.make_jabber_client('mcd_parser')

reactor.run()
