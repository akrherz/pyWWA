""" FFG """

# 3rd Party
from twisted.internet import reactor
from pyiem.util import LOG
from pyiem.nws.products.ffg import parser

# Local
from pywwa import common
from pywwa.ldm import bridge


def real_parser(txn, buf):
    """callback func"""
    ffg = parser(buf)
    if ffg.afos == "FFGMPD":
        return
    if common.dbwrite_enabled():
        ffg.sql(txn)
    if ffg.warnings and ffg.warnings[0].find("termination") == -1:
        common.email_error("\n".join(ffg.warnings), buf)
    sz = 0 if ffg.data is None else len(ffg.data.index)
    LOG.info("FFG found %s entries for product %s", sz, ffg.get_product_id())


def main():
    """Our main method"""
    bridge(real_parser, dbpool=common.get_database("postgis"))
    reactor.run()  # @UndefinedVariable


if __name__ == "__main__":
    main()
