""" XTEUS Product Parser! """
# 3rd Party
from pyiem.nws.products.xteus import parser
from twisted.internet import reactor

# Local
from pywwa import common
from pywwa.database import get_database
from pywwa.ldm import bridge

DBPOOL = get_database("iem")


def process_data(data):
    """Process the product"""
    try:
        prod = parser(data, utcnow=common.utcnow())
    except Exception as myexp:
        common.email_error(myexp, data)
        return None
    if prod.warnings:
        common.email_error("\n".join(prod.warnings), data)
    defer = DBPOOL.runInteraction(prod.sql)
    defer.addErrback(common.email_error, data)
    return prod


def main():
    """Fire things up."""
    common.main()
    bridge(process_data)
    reactor.run()


if __name__ == "__main__":
    # Go
    main()
