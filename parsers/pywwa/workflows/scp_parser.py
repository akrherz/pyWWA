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
    prod = parser(raw)
    if common.dbwrite_enabled():
        prod.sql(txn)


def main():
    """Go Main Go"""
    bridge(real_process, dbpool=get_database("asos"))
    reactor.run()  # @UndefinedVariable


if __name__ == "__main__":
    main()
