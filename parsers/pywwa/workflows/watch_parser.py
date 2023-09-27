""" SPC Watch (SAW, SEL, WWP) Ingestor """

# 3rd Party
from pyiem.nws.product import TextProduct
from pyiem.nws.products import parser
from pyiem.util import LOG
from twisted.internet import reactor
from twisted.internet.task import LoopingCall

# Local
from pywwa import common
from pywwa.database import get_database
from pywwa.ldm import bridge

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
    for txt, html, xtra in jmsgs:
        common.send_message(txt, html, xtra)


def process_queue():
    """Process the queue every minute to see if we have complete messages."""
    for wnum in list(QUEUE.keys()):
        entry = QUEUE[wnum]
        QUEUE[wnum]["loops"] += 1
        # 15 seconds, we await 4 minutes
        if QUEUE[wnum]["loops"] > 16:
            if entry["SAW"] is not None:
                do_jabber(entry)
            else:
                common.email_error(f"SAW Missing for {wnum}", repr(entry))
            QUEUE.pop(wnum)
            continue
        if None in [entry["SAW"], entry["SEL"], entry["WWP"]]:
            LOG.warning(
                "Watch %s[SAW:%s,SEL:%s,WWP:%s] is not complete, waiting",
                wnum,
                "miss" if entry["SAW"] is None else "X",
                "miss" if entry["SEL"] is None else "X",
                "miss" if entry["WWP"] is None else "X",
            )
            continue
        do_jabber(entry)
        QUEUE.pop(wnum)


def real_process(txn, raw) -> TextProduct:
    """Process the product, please"""
    prod = parser(raw, utcnow=common.utcnow())
    LOG.info("Watch %s received", prod.get_product_id())
    # NOTE: ensure parsers are implmenting the same interface
    if prod.is_test():
        LOG.info("TEST watch found %s, skipping", prod.get_product_id())
        return prod
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
    return prod


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
