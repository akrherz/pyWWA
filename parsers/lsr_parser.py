""" LSR product ingestor """
# Stdlib
import pickle
import os
import datetime

# 3rd Party
import pytz
from twisted.internet import reactor
from pyiem import reference
from pyiem.nws.products.lsr import parser as lsrparser
from pyiem.util import LOG
from pyldm import ldmbridge

# Local
from pywwa import common
from pywwa.xmpp import make_jabber_client

DBPOOL = common.get_database(common.CONFIG["databaserw"]["postgis"])

# Cheap datastore for LSRs to avoid Dups!
LSRDB = {}


def loaddb():
    """ load memory """
    if os.path.isfile("lsrdb.p"):
        mydict = pickle.load(open("lsrdb.p", "rb"))
        for key in mydict:
            LSRDB[key] = mydict[key]


def cleandb():
    """To keep LSRDB from growing too big, we clean it out
    Lets hold 7 days of data!
    """
    utc = datetime.datetime.utcnow().replace(tzinfo=pytz.utc)
    thres = utc - datetime.timedelta(hours=24 * 7)
    init_size = len(LSRDB)
    # keys() is now a generator, so generate all values before deletion
    for key in list(LSRDB.keys()):
        if LSRDB[key] < thres:
            del LSRDB[key]

    fin_size = len(LSRDB)
    LOG.info("cleandb() init_size: %s final_size: %s", init_size, fin_size)
    # Non blocking hackery
    reactor.callInThread(pickledb)

    # Call Again in 30 minutes
    reactor.callLater(60 * 30, cleandb)


def pickledb():
    """ Dump our database to a flat file """
    pickle.dump(LSRDB, open("lsrdb.p", "wb"))


class MyProductIngestor(ldmbridge.LDMProductReceiver):
    """ I process products handed off to me from faithful LDM """

    def process_data(self, data):
        """ @buf: String that is a Text Product """
        defer = DBPOOL.runInteraction(real_processor, data)
        defer.addErrback(common.email_error, data)

    def connectionLost(self, reason):
        """ the connection was lost! """
        common.shutdown()


def real_processor(txn, text):
    """ Lets actually process! """
    prod = lsrparser(text)

    for lsr in prod.lsrs:
        if lsr.typetext.upper() not in reference.lsr_events:
            errmsg = "Unknown LSR typecode '%s'" % (lsr.typetext,)
            common.email_error(errmsg, text)
        uniquekey = hash(lsr.text)
        if uniquekey in LSRDB:
            prod.duplicates += 1
            lsr.duplicate = True
            continue
        LSRDB[uniquekey] = datetime.datetime.utcnow().replace(tzinfo=pytz.utc)
        if common.dbwrite_enabled():
            lsr.sql(txn)

    j = prod.get_jabbers(common.SETTINGS.get("pywwa_lsr_url", "pywwa_lsr_url"))
    for i, (p, h, x) in enumerate(j):
        # delay some to perhaps stop triggering SPAM lock outs at twitter
        reactor.callLater(i, JABBER.send_message, p, h, x)

    if prod.warnings:
        common.email_error("\n\n".join(prod.warnings), text)
    elif not prod.lsrs:
        raise Exception("No LSRs parsed!", text)


reactor.callLater(0, loaddb)
JABBER = make_jabber_client("lsr_parser")
LDM = ldmbridge.LDMProductFactory(MyProductIngestor())
reactor.callLater(20, cleandb)
reactor.run()
