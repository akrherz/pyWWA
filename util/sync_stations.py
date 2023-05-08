"""Get station updates from IEM Webservice."""
# stdlib
import os
import sys

# Third Party
import requests

# Put the pywwa library into sys.path
sys.path.insert(0, os.path.join(os.path.abspath(__file__), "../parsers"))
# pylint: disable=wrong-import-position
from pywwa.database import get_sync_dbconn  # noqa: E402


def main():
    """Go Main Go."""
    pgconn = get_sync_dbconn("mesosite")
    cursor = pgconn.cursor()

    req = requests.get("http://mesonet.agron.iastate.edu/json/stations.php")
    jdata = req.json()
    for site in jdata["stations"]:
        if site["network"].find("ASOS") == -1:
            continue
        # Hmmm
        if site["wfo"] is None:
            site["wfo"] = ""
        cursor.execute(
            "SELECT iemid from stations WHERE id = %s and network = %s",
            (site["id"], site["network"]),
        )
        if cursor.rowcount == 0:
            cursor.execute(
                "INSERT into stations(id, network) values (%s, %s)",
                (site["id"], site["network"]),
            )
        cursor.execute(
            """
            UPDATE stations SET name = %s, state = %s, elevation = %s,
            geom = 'SRID=4326;POINT(%s %s)', county = %s, wfo = %s
            WHERE id = %s and network = %s
            """,
            (
                site["name"],
                site["state"],
                site["elevation"],
                float(site["lon"]),
                float(site["lat"]),
                site["county"],
                site["wfo"],
                site["id"],
                site["network"],
            ),
        )

    cursor.close()
    pgconn.commit()


if __name__ == "__main__":
    main()
