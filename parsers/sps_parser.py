"""SPS product ingestor"""

# Twisted Python imports
from syslog import LOG_LOCAL2
from twisted.python import syslog
syslog.startLogging(prefix='pyWWA/sps_parser', facility=LOG_LOCAL2)
from twisted.python import log
from twisted.internet import reactor

import datetime
import common
from pyiem.nws import product
from pyiem import reference
from pyldm import ldmbridge

from shapely.geometry import MultiPolygon

POSTGIS = common.get_database('postgis')
PYWWA_PRODUCT_URL = common.settings.get('pywwa_product_url',
                                        'pywwa_product_url')

ugc_dict = {}


def load_ugc(txn):
    """ load ugc dict """
    sql = """SELECT name, ugc from ugcs
        WHERE name IS NOT Null and end_ts is null"""
    txn.execute(sql)
    for row in txn:
        name = (row["name"]).replace("\x92", " ")
        ugc_dict[row['ugc']] = name

    log.msg("ugc_dict is loaded...")


def countyText(ugcs):
    """ Turn an array of UGC objects into string for message relay """
    countyState = {}
    c = ""
    for ugc in ugcs:
        stateAB = ugc.state
        if stateAB not in countyState:
            countyState[stateAB] = []
        if str(ugc) not in ugc_dict:
            name = "((%s))" % (str(ugc),)
        else:
            name = ugc_dict[str(ugc)]
        countyState[stateAB].append(name)

    for st in countyState.keys():
        countyState[stateAB].sort()
        c += " %s [%s] and" % (", ".join(countyState[st]), st)
    return c[:-4]


class myProductIngestor(ldmbridge.LDMProductReceiver):

    def process_data(self, buf):
        deffer = POSTGIS.runInteraction(real_process, buf)
        deffer.addErrback(common.email_error, buf)

    def connectionLost(self, reason):
        log.msg('connectionLost')
        log.err(reason)
        reactor.callLater(5, self.shutdown)

    def shutdown(self):
        reactor.callWhenRunning(reactor.stop)


def real_process(txn, raw):
    """ Really process! """
    if raw.find("$$") == -1:
        log.msg("$$ was missing from this product")
        raw += "\r\r\n$$\r\r\n"
    prod = product.TextProduct(raw)
    product_id = prod.get_product_id()
    xtra = {'product_id': product_id,
            'channels': ''}

    if prod.segments[0].sbw:
        ets = prod.valid + datetime.timedelta(hours=1)
        if len(prod.segments) > 0 and prod.segments[0].ugcexpire is not None:
            ets = prod.segments[0].ugcexpire
        giswkt = 'SRID=4326;%s' % (MultiPolygon([prod.segments[0].sbw]).wkt,)
        sql = """INSERT into text_products(product, product_id, geom,
            issue, expire) values (%s, %s, %s, %s, %s)"""
        myargs = (prod.unixtext, product_id, giswkt, prod.valid, ets)

    else:
        sql = "INSERT into text_products(product, product_id) values (%s,%s)"
        myargs = (prod.unixtext, product_id)
    txn.execute(sql, myargs)

    for seg in prod.segments:
        if len(seg.ugcs) == 0:
            continue
        headline = "[NO HEADLINE FOUND IN SPS]"
        if len(seg.headlines) > 0:
            headline = (seg.headlines[0]).replace("\n", " ")
        elif raw.find("SPECIAL WEATHER STATEMENT") > 0:
            headline = "Special Weather Statement"
        counties = countyText(seg.ugcs)
        if counties.strip() == "":
            counties = "entire area"

        expire = ""
        if seg.ugcexpire is not None:
            expire = "till %s %s" % (
                (seg.ugcexpire -
                 datetime.timedelta(hours=reference.offsets.get(prod.z, 0))
                 ).strftime("%-I:%M %p"), prod.z)
        xtra['channels'] = prod.afos
        mess = "%s issues %s for %s %s %s?pid=%s" % (prod.source[1:],
                                                     headline, counties,
                                                     expire, PYWWA_PRODUCT_URL,
                                                     product_id)
        htmlmess = ("<p>%s issues <a href='%s?pid=%s'>%s</a> for %s %s</p>"
                    ) % (prod.source[1:], PYWWA_PRODUCT_URL, product_id,
                         headline, counties, expire)
        xtra['twitter'] = "%s for %s %s %s?pid=%s" % (headline, counties,
                                                      expire,
                                                      PYWWA_PRODUCT_URL,
                                                      product_id)
        jabber.sendMessage(mess, htmlmess, xtra)

jabber = common.make_jabber_client('sps_parser')


def ready(bogus):
    ldmbridge.LDMProductFactory(myProductIngestor())


def killer(err):
    log.err(err)
    reactor.stop()

df = POSTGIS.runInteraction(load_ugc)
df.addCallback(ready)
df.addErrback(killer)

reactor.run()
