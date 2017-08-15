"""
  Dump some stuff without AFOS PILs
"""
import sys

from syslog import LOG_LOCAL2
from twisted.python import syslog
from twisted.python import log
from twisted.internet import reactor
import psycopg2
from pyiem.nws.product import TextProduct
from pyldm import ldmbridge
import common

syslog.startLogging(prefix='pyWWA/fake_afos_dump', facility=LOG_LOCAL2)


def compute_afos(textprod):
    """Our hackery to assign a fake AFOS pil to a product without AFOS"""
    ttaaii = textprod.wmo
    if ttaaii[:4] == 'NOXX':
        afos = 'ADM%s' % (textprod.source[1:], )
    elif ttaaii == 'FAUS20':
        afos = "MIS%s" % (textprod.source[1:], )
    elif ttaaii in ['FAUS21', 'FAUS22', 'FAUS23',  'FAUS24', 'FAUS25',
                    'FAUS26']:
        afos = "CWA%s" % (textprod.source[1:], )
    elif ttaaii[:4] == 'FOUS':
        afos = "FRH%s" % (ttaaii[4:], )
    elif ttaaii[:2] == 'FO' and ttaaii[2:4] in ['CA', 'UE', 'UM', 'CN', 'GX',
                                                'UW']:
        afos = "FRHT%s" % (ttaaii[4:], )
    elif ttaaii == 'URNT12' and textprod.source == 'KNHC':
        afos = "REPNT2"
    else:
        raise Exception("Unknown TTAAII %s conversion" % (ttaaii, ))

    textprod.afos = afos


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

    def process_data(self, data):
        """ Process the product """
        defer = PGCONN.runInteraction(really_process_data, data)
        defer.addErrback(common.email_error, data)
        defer.addErrback(log.err)


def really_process_data(txn, data):
    ''' We are called with a hard coded AFOS PIL '''
    tp = TextProduct(data)
    if tp.afos is None:
        compute_afos(tp)
    utc = tp.valid
    table = "products_%s_0106" % (utc.year,)
    if utc.month > 6:
        table = "products_%s_0712" % (utc.year,)

    sql = """INSERT into """ + table + """
        (pil, data, source, wmo, entered) values(%s,%s,%s,%s,%s)"""

    sqlargs = (tp.afos, tp.text,
               tp.source, tp.wmo, utc.strftime("%Y-%m-%d %H:%M+00"))
    txn.execute(sql, sqlargs)

    if tp.afos[:3] == 'FRH':
        return
    jmsgs = tp.get_jabbers(
        common.SETTINGS.get('pywwa_product_url', 'pywwa_product_url'))
    for jmsg in jmsgs:
        JABBER.send_message(*jmsg)


if __name__ == '__main__':
    PGCONN = common.get_database("afos", cp_max=1)
    JABBER = common.make_jabber_client('fake_afos_dump')
    ldmbridge.LDMProductFactory(MyProductIngestor())
    reactor.run()
