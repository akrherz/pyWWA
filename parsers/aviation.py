""" Aviation Product Parser! """
# stdlib
import os

# 3rd Party
from twisted.internet import reactor
from pyiem.util import LOG
from pyiem.nws.products.sigmet import parser

# Local
from pywwa import common
from pywwa.xmpp import make_jabber_client
from pywwa.ldm import bridge
from pywwa.database import get_database

DBPOOL = get_database("postgis")

# Load LOCS table
LOCS = {}
MESOSITE = get_database("mesosite")
JABBER = make_jabber_client()
_MYDIR = os.path.dirname(os.path.abspath(__file__))
TABLE_PATH = os.path.normpath(os.path.join(_MYDIR, "..", "tables"))


def load_database(txn):
    """Load database stations."""

    txn.execute(
        """
        SELECT id, name, ST_x(geom) as lon, ST_y(geom) as lat from stations
        WHERE network ~* 'ASOS' or network ~* 'AWOS'
        """
    )
    for row in txn.fetchall():
        LOCS[row["id"]] = row

    for line in open(TABLE_PATH + "/vors.tbl"):
        if len(line) < 70 or line[0] == "!":
            continue
        sid = line[:3]
        lat = float(line[56:60]) / 100.0
        lon = float(line[61:67]) / 100.0
        name = line[16:47].strip()
        LOCS[sid] = {"lat": lat, "lon": lon, "name": name}

    # Finally, GEMPAK!
    for line in open(TABLE_PATH + "/pirep_navaids.tbl"):
        if len(line) < 70 or line[0] in ["!", "#"]:
            continue
        sid = line[:3]
        lat = float(line[56:60]) / 100.0
        lon = float(line[61:67]) / 100.0
        LOCS[sid] = {"lat": lat, "lon": lon}


def process_data(data):
    """ Process the product """
    try:
        prod = parser(data, nwsli_provider=LOCS)
        # prod.draw()
    except Exception as myexp:
        common.email_error(myexp, data)
        return
    defer = DBPOOL.runInteraction(prod.sql)
    defer.addCallback(final_step, prod)
    defer.addErrback(common.email_error, data)


def final_step(_, prod):
    """send messages"""
    for j in prod.get_jabbers(
        common.SETTINGS.get("pywwa_product_url", "pywwa_product_url"), ""
    ):
        JABBER.send_message(j[0], j[1], j[2])


def onready(_res):
    """Database has loaded"""
    LOG.info("onready() called...")
    bridge(process_data)
    MESOSITE.close()


def bootstrap():
    """Fire things up."""
    df = MESOSITE.runInteraction(load_database)
    df.addCallback(onready)
    df.addErrback(common.email_error, "ERROR on load_database")
    df.addErrback(LOG.error)

    reactor.run()


if __name__ == "__main__":
    # Go
    bootstrap()
