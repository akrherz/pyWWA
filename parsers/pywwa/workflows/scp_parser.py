""" NESDIS SCP Ingestor """

# 3rd Party
from twisted.internet import reactor
from pyiem.nws.products.scp import parser

# Local
from pywwa import common
from pywwa.ldm import bridge
from pywwa.database import get_database


def real_process(txn, raw):
    """Process the product, please"""
    prod = parser(raw, utcnow=common.utcnow())
    if common.dbwrite_enabled():
        prod.sql(txn)
    if prod.warnings:
        common.email_error("\n".join(prod.warnings), raw)


def main():
    """Go Main Go"""
    common.main(with_jabber=False)
    bridge(real_process, dbpool=get_database("asos"))
    reactor.run()


if __name__ == "__main__":
    main()
