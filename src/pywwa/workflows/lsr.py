""" LSR product ingestor """
# Stdlib
import datetime
import os
import pickle

# 3rd Party
import click
from pyiem import reference
from pyiem.nws.products.lsr import parser as lsrparser
from pyiem.util import LOG, utc
from twisted.internet import reactor

# Local
from pywwa import common
from pywwa.database import get_database
from pywwa.ldm import bridge

# Cheap datastore for LSRs to avoid Dups!
LSRDB = {}


def loaddb():
    """load memory"""
    if os.path.isfile("lsrdb.p"):
        with open("lsrdb.p", "rb") as fh:
            mydict = pickle.load(fh)
        for key in mydict:
            LSRDB[key] = mydict[key]


def cleandb():
    """To keep LSRDB from growing too big, we clean it out
    Lets hold 7 days of data!
    """
    now = utc()
    thres = now - datetime.timedelta(hours=24 * 7)
    init_size = len(LSRDB)
    # loop safety here
    for key in list(LSRDB):
        if LSRDB[key] < thres:
            LSRDB.pop(key)

    fin_size = len(LSRDB)
    LOG.info("cleandb() init_size: %s final_size: %s", init_size, fin_size)
    # Non blocking hackery
    reactor.callInThread(pickledb)

    # Call Again in 30 minutes
    reactor.callLater(60 * 30, cleandb)


def pickledb():
    """Dump our database to a flat file"""
    with open("lsrdb.p", "wb") as fh:
        pickle.dump(LSRDB, fh)


def real_processor(txn, text):
    """Lets actually process!"""
    prod = lsrparser(text)

    for lsr in prod.lsrs:
        if lsr.typetext.upper() not in reference.lsr_events:
            errmsg = f"Unknown LSR typecode '{lsr.typetext}'"
            common.email_error(errmsg, text)
        uniquekey = hash(lsr.text)
        if uniquekey in LSRDB:
            prod.duplicates += 1
            # akrherz/pyWWA#150, This will modify database write below
            lsr.duplicate = True
        LSRDB[uniquekey] = utc()
        if common.dbwrite_enabled():
            lsr.sql(txn)

    j = prod.get_jabbers(common.SETTINGS.get("pywwa_lsr_url", "pywwa_lsr_url"))
    for i, (p, h, x) in enumerate(j):
        # delay some to perhaps stop triggering SPAM lock outs at twitter
        reactor.callLater(i, common.send_message, p, h, x)

    if prod.warnings:
        common.email_error("\n\n".join(prod.warnings), text)
    elif not prod.lsrs:
        raise Exception("No LSRs parsed!", text)


@click.command()
@common.init
def main(*args, **kwargs):
    """Go Main Go."""
    reactor.callLater(0, loaddb)
    bridge(real_processor, dbpool=get_database("postgis"))
    reactor.callLater(20, cleandb)
    reactor.run()
