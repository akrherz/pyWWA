""" HML parser! """

# 3rd Party
from twisted.internet import reactor
from pyiem.nws.products.hml import parser as hmlparser

# Local
from pywwa import common
from pywwa.ldm import bridge


def real_parser(txn, buf):
    """I'm gonna do the heavy lifting here"""
    prod = hmlparser(buf)
    if common.dbwrite_enabled():
        prod.sql(txn)
    if prod.warnings:
        common.email_error("\n".join(prod.warnings), buf)


if __name__ == "__main__":
    bridge(real_parser, dbpool=common.get_database("hml"))

    reactor.run()  # @UndefinedVariable
