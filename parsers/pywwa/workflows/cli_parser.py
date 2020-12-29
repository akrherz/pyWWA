"""Parse CLI text products

The CLI report has lots of good data that is hard to find in other products,
so we take what data we find in this product and overwrite the database
storage of what we got from the automated observations
"""

# 3rd Party
from twisted.internet import reactor
from pyiem.nws.products import parser
from pyiem.nws.products.cli import HARDCODED
from pyiem.network import Table as NetworkTable

# Local
from pywwa import common
from pywwa.xmpp import make_jabber_client
from pywwa.ldm import bridge
from pywwa.database import get_database

DBPOOL = get_database("iem")
NT = NetworkTable("NWSCLI", only_online=False)
HARDCODED["PKTN"] = "PAKT"
JABBER = make_jabber_client()


def send_tweet(prod):
    """ Send the tweet for this prod """

    jres = prod.get_jabbers(
        common.SETTINGS.get("pywwa_product_url", "pywwa_product_url")
    )
    for j in jres:
        JABBER.send_message(j[0], j[1], j[2])


def processor(txn, text):
    """ Protect the realprocessor """
    prod = parser(text, nwsli_provider=NT.sts, utcnow=common.utcnow())
    # Run through database save now
    prod.sql(txn)
    send_tweet(prod)
    return prod


def main():
    """Go Main Go."""
    bridge(processor, dbpool=DBPOOL)
    reactor.run()


if __name__ == "__main__":
    # Do Stuff
    main()
