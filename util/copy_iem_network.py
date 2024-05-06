"""For testing purposes, copy IEM network into mesosite."""

# stdlib
import sys

# Third Party
import httpx
from pywwa.database import get_dbconnc

SERVICE = "https://mesonet.agron.iastate.edu/geojson/network.py"


def main(argv):
    """Go Main Go."""
    pgconn, cursor = get_dbconnc("mesosite")
    network = argv[1]

    req = httpx.get(f"{SERVICE}?network={network}")
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
        giswkt = f"SRID=4326;POINT({lon} {lat})"
        cursor.execute(
            """
            UPDATE stations SET name = %s, state = %s, elevation = %s,
            geom = %s, county = %s, wfo = %s, tzname = %s
            WHERE id = %s and network = %s
            """,
            (
                site["sname"],
                site["state"],
                site["elevation"],
                giswkt,
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
