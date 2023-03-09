"""Parse FD text products."""

# 3rd Party
from twisted.internet import reactor
from pyiem.nws.products.fd import parser

# Local
from pywwa import common
from pywwa.ldm import bridge
from pywwa.database import get_database

DBPOOL = get_database("asos", cp_max=2)


def processor(txn, text):
    """Protect the realprocessor"""
    prod = parser(text, utcnow=common.utcnow())
    if common.dbwrite_enabled():
        prod.sql(txn)
    if prod.warnings:
        common.email_error("\n".join(prod.warnings), text)
    return prod


def main():
    """Go Main Go."""
    common.main(with_jabber=False)
    bridge(processor, dbpool=DBPOOL)
    reactor.run()


if __name__ == "__main__":
    # Do Stuff
    main()
