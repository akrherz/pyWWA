"""
Script that merges the upstream HVTEC NWSLI information into the database. We
are called like so

python merge_hvtec_nwsli.py hvtec_list_10132020.csv

Whereby the argument to the script is the filename stored here:

https://www.weather.gov/vtec/Valid-Time-Event-Code

hvtec_nwsli table:
 nwsli      | character(5)           |
 river_name | character varying(128) |
 proximity  | character varying(16)  |
 name       | character varying(128) |
 state      | character(2)           |
 geom       | geometry               |
"""

# stdlib
import sys

# 3rd Party
import httpx
from pyiem.util import logger

from pywwa.database import get_dbconnc

LOG = logger()


def main(argv) -> int:
    """Go Main"""
    if len(argv) < 2:
        print("USAGE: python merge_hvtec_nwsli.py FILENAME")
        return 1

    dbconn, cursor = get_dbconnc("postgis")
    LOG.warning(" - Connected to database: postgis")

    fn = argv[1]
    uri = f"https://www.weather.gov/media/vtec/{fn}"

    LOG.warning(" - Fetching file: %s", uri)
    req = httpx.get(uri, timeout=30)
    if req.status_code != 200:
        LOG.warning("Got status_code %s for %s", req.status_code, uri)
        return 1
    updated = 0
    new = 0
    bad = 0
    for linenum, line in enumerate(req.content.decode("ascii").split("\n")):
        if line.strip() == "":
            continue
        tokens = line.strip().split(",")
        if len(tokens) != 7:
            LOG.warning(
                " + Linenum %s had %s tokens, instead of 7\n%s",
                linenum + 1,
                len(tokens),
                line,
            )
            bad += 1
            continue
        (nwsli, river_name, proximity, name, state, lat, lon) = tokens
        if "\\N" in [lat, lon] or "" in [lat, lon]:
            LOG.warning(
                " + Linenum %s [%s] had a null lat/lon\n%s",
                linenum + 1,
                nwsli,
                line,
            )
            bad += 1
            continue
        if len(nwsli) != 5:
            LOG.warning(
                ' + Linenum %s had a NWSLI "%s" not of 5 character length\n%s',
                linenum + 1,
                nwsli,
                line,
            )
            bad += 1
            continue
        cursor.execute("DELETE from hvtec_nwsli WHERE nwsli = %s", (nwsli,))
        if cursor.rowcount == 1:
            updated += 1
        else:
            new += 1
        giswkt = f"SRID=4326;POINT({0 - float(lon)} {float(lat)})"
        sql = """
            INSERT into hvtec_nwsli (nwsli, river_name, proximity, name,
             state, geom) values (%s, %s, %s, %s, %s, %s)
             """
        args = (
            nwsli,
            river_name,
            proximity[:16],
            name,
            state,
            giswkt,
        )
        cursor.execute(sql, args)

    cursor.close()
    dbconn.commit()
    LOG.warning(
        " - DONE! %s updated %s new, %s bad entries", updated, new, bad
    )
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
