""" HML parser! """

# 3rd Party
from twisted.internet import reactor
from pyiem.nws.products.hml import parser as hmlparser

# Local
from pywwa import common
from pywwa.ldm import bridge
from pywwa.database import get_database


def real_parser(txn, buf):
    """I'm gonna do the heavy lifting here"""
    prod = hmlparser(buf)
    if common.dbwrite_enabled():
        prod.sql(txn)
    if prod.warnings:
        common.email_error("\n".join(prod.warnings), buf)


def main():
    """Go Main Go."""
    bridge(real_parser, dbpool=get_database("hml"))
    reactor.run()  # @UndefinedVariable


if __name__ == "__main__":
    main()
