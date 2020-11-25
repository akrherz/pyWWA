"""Parse CF6 text products."""

# 3rd Party
from twisted.internet import reactor
from pyiem.nws.products.cf6 import parser

# Local
from pywwa import common
from pywwa.ldm import bridge

DBPOOL = common.get_database("iem")


def processor(txn, text):
    """ Protect the realprocessor """
    prod = parser(text)
    if common.dbwrite_enabled():
        prod.sql(txn)


if __name__ == "__main__":
    # Do Stuff
    bridge(processor, dbpool=DBPOOL)

    reactor.run()
