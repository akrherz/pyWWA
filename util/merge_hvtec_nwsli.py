"""
Script that merges the upstream HVTEC NWSLI information into the database. We
are called like so

python merge_hvtec_nwsli.py hvtec_list_07092019.csv

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
from __future__ import print_function
import sys

import requests
from pyiem.util import get_dbconn, logger


def main(argv):
    """Go Main"""
    log = logger()
    if len(argv) < 2:
        print("USAGE: python merge_hvtec_nwsli.py FILENAME")
        return

    dbconn = get_dbconn("postgis", user="mesonet")
    cursor = dbconn.cursor()
    log.info(" - Connected to database: postgis")

    fn = argv[1]
    uri = "https://www.weather.gov/media/vtec/%s" % (fn,)

    log.info(" - Fetching file: %s", uri)
    req = requests.get(uri)
    updated = 0
    new = 0
    bad = 0
    for linenum, line in enumerate(req.content.decode("ascii").split("\n")):
        if line.strip() == "":
            continue
        tokens = line.strip().split(",")
        if len(tokens) != 7:
            log.info(
                " + Linenum %s had %s tokens, instead of 7\n%s",
                linenum + 1,
                len(tokens),
                line,
            )
            bad += 1
            continue
        (nwsli, river_name, proximity, name, state, lat, lon) = tokens
        if len(nwsli) != 5:
            log.info(
                ' + Linenum %s had a NWSLI "%s" not of 5 character length\n%s',
                linenum + 1,
                nwsli,
                line,
            )
            bad += 1
            continue
        cursor.execute(
            """
            DELETE from hvtec_nwsli WHERE nwsli = %s
        """,
            (nwsli,),
        )
        if cursor.rowcount == 1:
            updated += 1
        else:
            new += 1
        sql = """
            INSERT into hvtec_nwsli (nwsli, river_name, proximity, name,
             state, geom) values (%s, %s, %s, %s, %s,
             'SRID=4326;POINT(%s %s)')
             """
        args = (
            nwsli,
            river_name,
            proximity,
            name,
            state,
            0 - float(lon),
            float(lat),
        )
        cursor.execute(sql, args)

    cursor.close()
    dbconn.commit()
    log.info(" - DONE! %s updated %s new, %s bad entries", updated, new, bad)


if __name__ == "__main__":
    main(sys.argv)
