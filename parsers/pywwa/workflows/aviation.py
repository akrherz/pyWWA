""" Aviation Product Parser! """
# 3rd Party
from pyiem.nws.products.sigmet import parser
from pyiem.util import LOG
from twisted.internet import reactor

# Local
from pywwa import common, get_table_file
from pywwa.database import get_database
from pywwa.ldm import bridge

DBPOOL = get_database("postgis")

# Load LOCS table
LOCS = {}
MESOSITE = get_database("mesosite")


def goodline(line):
    """Is this line any good?"""
    return len(line) > 69 and line[0] not in ["!", "#"]


def load_database(txn):
    """Load database stations."""

    txn.execute(
        "SELECT id, name, ST_x(geom) as lon, ST_y(geom) as lat from stations "
        "WHERE network ~* 'ASOS' or network ~* 'AWOS'"
    )
    for row in txn.fetchall():
        LOCS[row["id"]] = row

    for line in filter(goodline, get_table_file("vors.tbl")):
        sid = line[:3]
        lat = float(line[56:60]) / 100.0
        lon = float(line[61:67]) / 100.0
        name = line[16:47].strip()
        LOCS[sid] = {"lat": lat, "lon": lon, "name": name}

    # Finally, GEMPAK!
    for line in filter(goodline, get_table_file("pirep_navaids.tbl")):
        sid = line[:3]
        lat = float(line[56:60]) / 100.0
        lon = float(line[61:67]) / 100.0
        LOCS[sid] = {"lat": lat, "lon": lon}


def process_data(data):
    """Process the product"""
    try:
        prod = parser(data, nwsli_provider=LOCS, utcnow=common.utcnow())
    except Exception as myexp:
        common.email_error(myexp, data)
        return None
    if prod.warnings:
        common.email_error("\n".join(prod.warnings), data)
    defer = DBPOOL.runInteraction(prod.sql)
    defer.addCallback(final_step, prod)
    defer.addErrback(common.email_error, data)
    return prod


def final_step(_, prod):
    """send messages"""
    for j in prod.get_jabbers(
        common.SETTINGS.get("pywwa_product_url", "pywwa_product_url"), ""
    ):
        common.send_message(j[0], j[1], j[2])


def onready(_res):
    """Database has loaded"""
    LOG.info("onready() called...")
    bridge(process_data)
    MESOSITE.close()


def main():
    """Fire things up."""
    common.main()
    df = MESOSITE.runInteraction(load_database)
    df.addCallback(onready)
    df.addErrback(common.shutdown)
    reactor.run()


if __name__ == "__main__":
    # Go
    main()
