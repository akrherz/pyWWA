""" SPC Watch (SAW, SEL, WWP) Ingestor """

# 3rd Party
from twisted.internet import reactor
from twisted.internet.task import LoopingCall
from pyiem.util import LOG
from pyiem.nws.products import parser

# Local
from pywwa import common
from pywwa.ldm import bridge
from pywwa.database import get_database

# Create a running queue of watch products, so that once we get the
# collective, we can generate the jabbers and send them out.
QUEUE = {}


def do_jabber(entry):
    """We are ready now, send out the jabbers"""
    baseurl = common.SETTINGS.get("pywwa_watch_url", "pywwa_watch_url")
    prod = entry["SAW"]
    jmsgs = prod.get_jabbers(
        baseurl,
        wwpprod=entry["WWP"],
        selprod=entry["SEL"],
    )
    for (txt, html, xtra) in jmsgs:
        common.send_message(txt, html, xtra)


def process_queue():
    """Process the queue every minute to see if we have complete messages."""
    for wnum in list(QUEUE.keys()):
        entry = QUEUE[wnum]
        QUEUE[wnum]["loops"] += 1
        # 15 seconds, we await 4 minutes
        if QUEUE[wnum]["loops"] > 16:
            do_jabber(entry)
            QUEUE.pop(wnum)
            continue
        if None in [entry["SAW"], entry["SEL"], entry["WWP"]]:
            LOG.warning("Watch %s is not complete, waiting", wnum)
            continue
        do_jabber(entry)
        QUEUE.pop(wnum)


def real_process(txn, raw):
    """Process the product, please"""
    prod = parser(raw)
    LOG.info("Watch %s received", prod.get_product_id())
    # NOTE: insure parsers are implmenting the same interface
    if prod.is_test():
        LOG.info("TEST watch found %s, skipping", prod.get_product_id())
        return
    if common.dbwrite_enabled():
        prod.sql(txn)
    if prod.warnings:
        common.email_error("\n".join(prod.warnings), raw)

    if prod.afos[:3] != "SAW":
        wnum = prod.data.num
    else:
        prod.compute_wfos(txn)
        wnum = prod.ww_num

    res = QUEUE.setdefault(
        wnum, {"SAW": None, "SEL": None, "WWP": None, "loops": 0}
    )
    res[prod.afos[:3]] = prod


def main():
    """Go Main Go"""
    common.main()
    bridge(real_process, dbpool=get_database("postgis"))
    lc = LoopingCall(process_queue)
    df = lc.start(15, now=False)
    df.addErrback(common.email_error)
    reactor.run()


if __name__ == "__main__":
    main()
