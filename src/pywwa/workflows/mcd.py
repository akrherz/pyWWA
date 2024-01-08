"""
 Support SPC's MCD product
 Support WPC's FFG product
"""

# 3rd Party
import click
from pyiem.nws.products.mcd import parser as mcdparser

# Local
from pywwa import common
from pywwa.database import get_database
from pywwa.ldm import bridge

DBPOOL = get_database("postgis")


def process_data(data):
    """Process a chunk of data"""
    # BUG
    data = data.upper()
    df = DBPOOL.runInteraction(real_process, data)
    df.addErrback(common.email_error, data)


def find_cwsus(txn, prod):
    """
    Provided a database transaction, go look for CWSUs that
    overlap the discussion geometry.
    ST_Overlaps do the geometries overlap
    ST_Covers does polygon exist inside CWSU
    """
    wkt = f"SRID=4326;{prod.geometry.wkt}"
    txn.execute(
        "select distinct id from cwsu WHERE st_overlaps(%s, geom) or "
        "st_covers(geom, %s) ORDER by id ASC",
        (wkt, wkt),
    )
    cwsus = []
    for row in txn.fetchall():
        cwsus.append(row["id"])
    return cwsus


def real_process(txn, raw):
    """ "
    Actually process a single MCD
    """
    prod = mcdparser(raw)
    prod.cwsus = find_cwsus(txn, prod)

    j = prod.get_jabbers(
        common.SETTINGS.get("pywwa_product_url", "pywwa_product_url")
    )
    if len(j) == 1:
        common.send_message(j[0][0], j[0][1], j[0][2])
    if common.dbwrite_enabled():
        prod.database_save(txn)
    if prod.warnings:
        common.email_error("\n".join(prod.warnings), raw)


@click.command()
@common.init
def main(*args, **kwargs):
    """Go Main Go."""
    bridge(process_data)
