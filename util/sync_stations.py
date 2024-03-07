"""Get station updates from IEM Webservice."""

# Third Party
import requests
from pywwa.database import get_dbconnc


def main():
    """Go Main Go."""
    pgconn, cursor = get_dbconnc("mesosite")

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
        giswkt = f"SRID=4326;POINT({site['lon']} {site['lat']})"
        cursor.execute(
            """
            UPDATE stations SET name = %s, state = %s, elevation = %s,
            geom = %s, county = %s, wfo = %s
            WHERE id = %s and network = %s
            """,
            (
                site["name"],
                site["state"],
                site["elevation"],
                giswkt,
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
