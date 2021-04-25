""" PIREP parser! """
# stdlib
import datetime

# 3rd Party
from twisted.internet import reactor
from pyiem.util import LOG
from pyiem.nws.products.pirep import parser as pirepparser

# Local
from pywwa import common, get_table_file
from pywwa.ldm import bridge
from pywwa.database import get_database

PIREPS = {}
DBPOOL = get_database("postgis")
# Load LOCS table
LOCS = {}


def cleandb():
    """To keep LSRDB from growing too big, we clean it out
    Lets hold 1 days of data!
    """
    thres = datetime.datetime.utcnow() - datetime.timedelta(hours=24 * 1)
    init_size = len(PIREPS.keys())
    # loop safety
    for key in list(PIREPS):
        if PIREPS[key] < thres:
            PIREPS.pop(key)

    fin_size = len(PIREPS.keys())
    LOG.info("cleandb() init_size: %s final_size: %s", init_size, fin_size)

    # Call Again in 30 minutes
    reactor.callLater(60 * 30, cleandb)  # @UndefinedVariable


def load_locs(txn):
    """Build locations table"""
    LOG.info("load_locs() called...")
    txn.execute(
        "SELECT id, name, st_x(geom) as lon, st_y(geom) as lat "
        "from stations WHERE network ~* 'ASOS' or network ~* 'AWOS'"
    )
    for row in txn.fetchall():
        LOCS[row["id"]] = {
            "id": row["id"],
            "name": row["name"],
            "lon": row["lon"],
            "lat": row["lat"],
        }

    for line in get_table_file("faa_apt.tbl"):
        if len(line) < 70 or line[0] == "!":
            continue
        sid = line[:4].strip()
        lat = float(line[56:60]) / 100.0
        lon = float(line[61:67]) / 100.0
        name = line[16:47].strip()
        if sid not in LOCS:
            LOCS[sid] = {"lat": lat, "lon": lon, "name": name}

    for line in get_table_file("vors.tbl"):
        if len(line) < 70 or line[0] == "!":
            continue
        sid = line[:3]
        lat = float(line[56:60]) / 100.0
        lon = float(line[61:67]) / 100.0
        name = line[16:47].strip()
        if sid not in LOCS:
            LOCS[sid] = {"lat": lat, "lon": lon, "name": name}

    # Finally, GEMPAK!
    for line in get_table_file("pirep_navaids.tbl"):
        if len(line) < 60 or line[0] in ["!", "#"]:
            continue
        sid = line[:4].strip()
        lat = float(line[56:60]) / 100.0
        lon = float(line[61:67]) / 100.0
        if sid not in LOCS:
            LOCS[sid] = {"lat": lat, "lon": lon}

    LOG.info("... %s locations loaded", len(LOCS))


def real_parser(txn, buf):
    """
    I'm gonna do the heavy lifting here
    """
    prod = pirepparser(buf, nwsli_provider=LOCS)
    prod.assign_cwsu(txn)
    for report in prod.reports:
        if report.text in PIREPS:
            report.is_duplicate = True
        PIREPS[report.text] = datetime.datetime.utcnow()

    j = prod.get_jabbers("unused")
    if prod.warnings:
        common.email_error("\n".join(prod.warnings), buf)
    for msg in j:
        common.send_message(msg[0], msg[1], msg[2])
    if common.dbwrite_enabled():
        prod.sql(txn)


def ready(_bogus):
    """We are ready to ingest"""
    reactor.callLater(20, cleandb)  # @UndefinedVariable
    bridge(real_parser, dbpool=DBPOOL)


def main():
    """GO Main Go."""
    common.main()
    df = DBPOOL.runInteraction(load_locs)
    df.addCallback(ready)
    df.addErrback(common.shutdown)

    reactor.run()  # @UndefinedVariable


if __name__ == "__main__":
    main()
