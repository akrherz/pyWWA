"""SPENES product ingestor"""
# Twisted Python imports
from syslog import LOG_LOCAL2
from twisted.python import syslog
from twisted.python import log
syslog.startLogging(prefix='pyWWA/spe_parser', facility=LOG_LOCAL2)
from twisted.internet import reactor

import re
import common
# pyLDM https://github.com/akrherz/pyLDM
from pyldm import ldmbridge


from pyiem.nws import product
POSTGIS = common.get_database('postgis', cp_max=1)
PYWWA_PRODUCT_URL = common.settings.get('pywwa_product_url',
                                        'pywwa_product_url')
DB_ON = bool(common.settings.get('pywwa_save_text_products', False))


def shutdown():
    ''' Stop this app '''
    log.msg("Shutting down...")
    reactor.callWhenRunning(reactor.stop)


class MyProductIngestor(ldmbridge.LDMProductReceiver):
    """ I receive products from ldmbridge and process them 1 by 1 :) """

    def connectionLost(self, reason):
        ''' callback when the stdin reader connection is closed '''
        log.msg('connectionLost() called...')
        log.err(reason)
        reactor.callLater(7, shutdown)

    def process_data(self, buf):
        """ Process the product """
        df = POSTGIS.runInteraction(real_process, buf)
        df.addErrback(common.email_error, buf)
        df.addErrback(log.err)


def real_process(txn, raw):
    sqlraw = raw.replace("\015\015\012", "\n")
    prod = product.TextProduct(raw)

    product_id = prod.get_product_id()
    if DB_ON:
        sql = """
            INSERT into text_products(product, product_id) values (%s,%s)
        """
        myargs = (sqlraw, product_id)
        txn.execute(sql, myargs)

    tokens = re.findall("ATTN (WFOS|RFCS)(.*)", raw)
    channels = []
    for tpair in tokens:
        for center in re.findall("([A-Z]+)\.\.\.", tpair[1]):
            channels.append("SPENES.%s" % (center, ))
    xtra = {'product_id': product_id}
    xtra['channels'] = ','.join(channels)
    xtra['twitter'] = ("NESDIS issues Satellite Precipitation "
                       "Estimates %s?pid=%s") % (PYWWA_PRODUCT_URL,
                                                 product_id)

    body = ("NESDIS issues Satellite Precipitation Estimates %s?pid=%s"
            ) % (PYWWA_PRODUCT_URL, product_id)
    htmlbody = ("<p>NESDIS issues "
                "<a href='%s?pid=%s'>Satellite Precipitation Estimates</a>"
                "</p>"
                ) % (PYWWA_PRODUCT_URL, product_id)
    jabber.sendMessage(body, htmlbody, xtra)


def killer():
    reactor.stop()

if __name__ == '__main__':
    jabber = common.make_jabber_client("spe_parser")
    ldmbridge.LDMProductFactory(MyProductIngestor(dedup=True))
    reactor.run()
