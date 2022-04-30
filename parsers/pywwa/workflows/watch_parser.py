""" SPC Watch (SAW, WWP) Ingestor """

# 3rd Party
from twisted.internet import reactor
from pyiem.util import LOG
from pyiem.nws.products.saw import parser as sawparser
from pyiem.nws.products.wwp import parser as wwpparser

# Local
from pywwa import common
from pywwa.ldm import bridge
from pywwa.database import get_database


def real_process(txn, raw):
    """Process the product, please"""
    iswwp = False
    if raw.find("PROBABILITY TABLE") > -1:
        prod = wwpparser(raw)
        iswwp = True
    else:
        prod = sawparser(raw)
    # NOTE: insure parsers are implmenting the same interface
    if prod.is_test():
        LOG.info("TEST watch found %s, skipping", prod.get_product_id())
        return
    if common.dbwrite_enabled():
        prod.sql(txn)
    if iswwp:
        return
    prod.compute_wfos(txn)
    baseurl = common.SETTINGS.get("pywwa_watch_url", "pywwa_watch_url")
    for (txt, html, xtra) in prod.get_jabbers(baseurl):
        common.send_message(txt, html, xtra)
    if prod.warnings:
        common.email_error("\n".join(prod.warnings), raw)


def main():
    """Go Main Go"""
    common.main()
    bridge(real_process, dbpool=get_database("postgis"))
    reactor.run()


if __name__ == "__main__":
    main()
