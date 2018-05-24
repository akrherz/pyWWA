"""SPS product ingestor"""
import datetime

from twisted.python import log
from twisted.internet import reactor
from shapely.geometry import MultiPolygon
from pyiem.nws import product
from pyiem import reference
from pyldm import ldmbridge
import common

POSTGIS = common.get_database('postgis')
PYWWA_PRODUCT_URL = common.SETTINGS.get('pywwa_product_url',
                                        'pywwa_product_url')

ugc_dict = {}


def load_ugc(txn):
    """ load ugc dict """
    # Careful here not to load things from the future
    txn.execute("""
        SELECT name, ugc, wfo from ugcs WHERE
        name IS NOT Null and begin_ts < now() and
        (end_ts is null or end_ts > now())
    """)
    for row in txn.fetchall():
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

    for st in countyState:
        countyState[st].sort()
        c += " %s [%s] and" % (", ".join(countyState[st]), st)
    return c[:-4]


class myProductIngestor(ldmbridge.LDMProductReceiver):
    """My ingestor"""

    def process_data(self, data):
        """Got data"""
        deffer = POSTGIS.runInteraction(real_process, data)
        deffer.addErrback(common.email_error, data)

    def connectionLost(self, reason):
        """stdin was closed"""
        log.msg('connectionLost')
        log.err(reason)
        reactor.callLater(5, self.shutdown)

    def shutdown(self):
        """We want to shutdown"""
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
        if prod.segments and prod.segments[0].ugcexpire is not None:
            ets = prod.segments[0].ugcexpire
        giswkt = 'SRID=4326;%s' % (MultiPolygon([prod.segments[0].sbw]).wkt,)
        sql = """
            INSERT into text_products(product, product_id, geom,
            issue, expire, pil) values (%s, %s, %s, %s, %s, %s)
        """
        myargs = (prod.unixtext, product_id, giswkt, prod.valid, ets,
                  prod.afos)

    else:
        sql = "INSERT into text_products(product, product_id) values (%s,%s)"
        myargs = (prod.unixtext, product_id)
    txn.execute(sql, myargs)

    for seg in prod.segments:
        if seg.ugcs:
            continue
        headline = "[NO HEADLINE FOUND IN SPS]"
        if seg.headlines:
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
        jabber.send_message(mess, htmlmess, xtra)


jabber = common.make_jabber_client('sps_parser')


def ready(_bogus):
    """we are ready to go"""
    ldmbridge.LDMProductFactory(myProductIngestor())


def killer(err):
    """hard stop"""
    log.err(err)
    reactor.stop()


df = POSTGIS.runInteraction(load_ugc)
df.addCallback(ready)
df.addErrback(killer)

reactor.run()
