""" HML parser! """

# 3rd Party
from pyiem.nws.products.hml import parser as hmlparser
from twisted.internet import reactor

# Local
from pywwa import common
from pywwa.database import get_database
from pywwa.ldm import bridge


def real_parser(txn, buf):
    """I'm gonna do the heavy lifting here"""
    prod = hmlparser(buf)
    if common.dbwrite_enabled():
        prod.sql(txn)
    if prod.warnings:
        common.email_error("\n".join(prod.warnings), buf)


def main():
    """Go Main Go."""
    common.main(with_jabber=False)
    bridge(real_parser, dbpool=get_database("hml"))
    reactor.run()  # @UndefinedVariable


if __name__ == "__main__":
    main()
