"""For testing purposes, copy IEM network into mesosite."""
# stdlib
import os
import sys

# Third Party
import requests

# Put the pywwa library into sys.path
sys.path.insert(0, os.path.join(os.path.abspath(__file__), "../parsers"))
# pylint: disable=wrong-import-position
from pywwa.database import get_sync_dbconn  # noqa: E402

SERVICE = "https://mesonet.agron.iastate.edu/geojson/network.py"


def main(argv):
    """Go Main Go."""
    pgconn = get_sync_dbconn("mesosite")
    cursor = pgconn.cursor()
    network = argv[1]

    req = requests.get(f"{SERVICE}?network={network}")
    jdata = req.json()
    for feat in jdata["features"]:
        site = feat["properties"]
        [lon, lat] = feat["geometry"]["coordinates"]
        if site["wfo"] is None:
            site["wfo"] = ""
        cursor.execute(
            "SELECT iemid from stations WHERE id = %s and network = %s",
            (site["sid"], network),
        )
        if cursor.rowcount == 0:
            cursor.execute(
                "INSERT into stations(id, network) values (%s, %s)",
                (site["sid"], network),
            )
        cursor.execute(
            """
            UPDATE stations SET name = %s, state = %s, elevation = %s,
            geom = 'SRID=4326;POINT(%s %s)', county = %s, wfo = %s, tzname = %s
            WHERE id = %s and network = %s
            """,
            (
                site["sname"],
                site["state"],
                site["elevation"],
                lon,
                lat,
                site["county"],
                site["wfo"],
                site["tzname"],
                site["sid"],
                network,
            ),
        )

    cursor.close()
    pgconn.commit()


if __name__ == "__main__":
    main(sys.argv)
