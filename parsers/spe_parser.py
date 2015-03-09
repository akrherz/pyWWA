"""SPENES product ingestor

I am not longterm running, but exec once per product!
"""
# Twisted Python imports
from syslog import LOG_LOCAL2
from twisted.python import syslog
syslog.startLogging(prefix='pyWWA/spe_parser', facility=LOG_LOCAL2)
from twisted.internet import reactor

import sys
import re
import common

from pyiem.nws import product
POSTGIS = common.get_database('postgis', cp_max=1)
raw = sys.stdin.read()
PYWWA_PRODUCT_URL = common.settings.get('pywwa_product_url',
                                        'pywwa_product_url')


def process(raw):
    try:
        real_process(raw)
    except Exception, exp:
        common.email_error(exp, raw)


def real_process(raw):
    sqlraw = raw.replace("\015\015\012", "\n")
    prod = product.TextProduct(raw)

    product_id = prod.get_product_id()
    sql = """INSERT into text_products(product, product_id) values (%s,%s)"""
    myargs = (sqlraw, product_id)
    POSTGIS.runOperation(sql, myargs)

    tokens = re.findall("ATTN (WFOS|RFCS)(.*)", raw)
    for tpair in tokens:
        wfos = re.findall("([A-Z]+)\.\.\.", tpair[1])
        # Remember, xtra is passed below by reference, so we need to reinit it
        xtra = {'product_id': product_id}
        xtra['channels'] = ','.join(wfos)
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
    reactor.callLater(0, process, raw)
    reactor.callLater(30, killer)
    reactor.run()
