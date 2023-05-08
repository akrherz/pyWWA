"""Parse CF6 text products."""

# 3rd Party
from pyiem.nws.products.cf6 import parser
from twisted.internet import reactor

# Local
from pywwa import common
from pywwa.database import get_database
from pywwa.ldm import bridge

DBPOOL = get_database("iem")


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
