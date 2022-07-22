"""Process WPC Excessive Rainfall Outlook (ERO) GeoJSON.

  - Ingest GeoJSON into database.
  - Submit to LDM for archival.

https://origin.wpc.ncep.noaa.gov/exper/eromap/geojson/
"""
from datetime import timezone
import json
import os
import subprocess
import tempfile

import pandas as pd
import geopandas as gpd
import requests
import xmpp
from pyiem.nws.products import ero
from pyiem.util import get_dbconn, get_dbconnstr, utc, logger, get_properties

LOG = logger()
# No akami
BASEURI = "https://origin.wpc.ncep.noaa.gov/exper/eromap/geojson/"
JABBER = {
    "connection": None,
    "to": None,
}


def get_jabberconn():
    """Fetch or initialize a jabber connection."""
    if JABBER["connection"] is not None:
        return JABBER["connection"]
    props = get_properties()
    JABBER["to"] = f"{props['bot.username']}@{props['bot.xmppdomain']}"
    jid = xmpp.protocol.JID(
        f"{props['pywwa_jabber_username']}@{props['pywwa_jabber_domain']}/"
        "ero_ingest"
    )
    JABBER["connection"] = xmpp.Client(
        props.get("pywwa_jabber_host", "localhost"),
        debug=[],
    )
    JABBER["connection"].connect()
    JABBER["connection"].auth(
        user=jid.getNode(),
        password=props.get("pywwa_jabber_password"),
        resource=jid.getResource(),
    )
    return JABBER["connection"]


def compute_cycle(day, valid):
    """Figure out an integer cycle that identifies this product."""
    if day == 1:
        if valid.hour in range(0, 6):
            return 1
        if valid.hour in range(6, 13):
            return 8
        if valid.hour in range(13, 20):
            return 16
    if valid.hour in range(4, 12):
        return 8
    if valid.hour in range(17, 22):
        return 20
    return -1


def fetch_ero(day) -> gpd.GeoDataFrame:
    """Get the ERO from the WPC website."""
    gdf = gpd.read_file(f"{BASEURI}EROday{day}.geojson")
    # Uppercase all the column names
    gdf.columns = [x.upper() if x != "geometry" else x for x in gdf.columns]
    cols = ["ISSUE_TIME", "START_TIME", "END_TIME"]
    for col in cols:
        gdf[col] = pd.to_datetime(
            gdf[col], format="%Y-%m-%d %H:%M:%S"
        ).dt.tz_localize(timezone.utc)
    # Instead of having an empty geojson, they have a bogus polygon with
    # the properties
    if gdf.empty:
        LOG.info("Empty GeoJSON for day %s", day)
        return None, None
    # Some value add properties that are useful for downstream processing
    gdf["day"] = day
    gdf["threshold"] = ""
    # Save metdata for first row
    meta = gdf.iloc[0][cols].copy()
    gdf = gdf[gdf["OUTLOOK"] != "None Expected"]
    return gdf, meta


def send_to_ldm(gdf, meta, maxissue, day, cycle):
    """Insert for archival and triggering."""
    if cycle < 0:
        LOG.info("Skipping LDM as this is non-regular cycle (-1)")
        return
    for column in gdf.columns:
        try:
            gdf[column] = gdf[column].dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        except Exception:
            pass

    with tempfile.NamedTemporaryFile(delete=False) as tmpfn:
        if gdf.empty:
            tmpfn.write(b'{"type": "FeatureCollection", "features": []}')
        else:
            gdf.to_file(tmpfn.name, driver="GeoJSON")
    # Hack back in some properties
    with open(tmpfn.name, "r", encoding="utf-8") as tmpfh:
        jdict = json.load(tmpfh)
    jdict["iem_properties"] = meta.to_dict()
    for key, value in jdict["iem_properties"].items():
        try:
            jdict["iem_properties"][key] = value.strftime("%Y-%m-%dT%H:%M:%SZ")
        except Exception:
            pass

    # Write back out
    with open(tmpfn.name, "w", encoding="utf-8") as tmpfh:
        json.dump(jdict, tmpfh)

    cmd = (
        f"pqinsert -i -p 'data ac {maxissue:%Y%m%d%H%M} gis/geojson/wpc_ero/"
        f"eroday{day}.geojson GIS/wpc_ero/eroday{day}_{cycle}z.geojson "
        f"geojson' {tmpfn.name}"
    )
    LOG.info(cmd)
    subprocess.call(cmd, shell=True)
    os.unlink(tmpfn.name)


def get_threshold(threshold):
    """Convert to key."""
    thres = threshold.lower()
    if thres.startswith("slight"):
        return "SLGT"
    if thres.startswith("moderate"):
        return "MDT"
    if thres.startswith("high"):
        return "HIGH"
    if thres.startswith("marginal"):
        return "MRGL"
    LOG.info("Failed to convert '%s' to threshold", threshold)
    return None


def save_df(cursor, gdf, meta, day, cycle):
    """Save to the database."""
    # Create the identifier for this outlook
    cursor.execute(
        "INSERT into spc_outlook(issue, product_issue, expire, updated, "
        "product_id, outlook_type, day, cycle) VALUES "
        "(%s, %s, %s, now(), %s, 'E', %s, %s) RETURNING id",
        (
            meta["START_TIME"],
            meta["ISSUE_TIME"],
            meta["END_TIME"],
            "GEOJSON",
            day,
            cycle,
        ),
    )
    oid = cursor.fetchone()[0]
    for idx, row in gdf.iterrows():
        threshold = get_threshold(row["OUTLOOK"])
        gdf.at[idx, "threshold"] = threshold
        LOG.info(
            "Adding %s[%s] %s size: %.4f",
            day,
            cycle,
            threshold,
            row["geometry"].area,
        )
        cursor.execute(
            "INSERT into spc_outlook_geometries (spc_outlook_id, threshold, "
            "category, geom) VALUES (%s, %s, 'CATEGORICAL', "
            "ST_SetSrid(ST_Multi(ST_GeomFromEWKT(%s)), 4326))",
            (
                oid,
                threshold,
                row["geometry"].wkt,
            ),
        )


def send_jabber(gdf, issue, day):
    """Do the jabber message generation,"""
    # Create a faked outlook_collections list
    outlook_collections = {
        day: ero.OutlookCollection(
            "issue unused",
            "expire unused",
            day,
        )
    }
    for _, row in gdf.iterrows():
        outlook = ero.Outlook(
            "CATEGORICAL",
            row["threshold"],
            row["geometry"],
        )
        outlook_collections[day].outlooks.append(outlook)

    ero.compute_wfos(outlook_collections)
    jmsgs = ero.jabber_messages(
        issue,
        outlook_collections,
    )
    LOG.info("Sending %d jabber messages", len(jmsgs))
    for (txt, html, xtra) in jmsgs:
        conn = get_jabberconn()
        msg = xmpp.protocol.Message(
            to=JABBER["to"],
            body=txt,
        )
        xhtml = xmpp.Node(
            "html", {"xmlns": "http://jabber.org/protocol/xhtml-im"}
        )
        xhtml.addChild(
            node=xmpp.simplexml.XML2Node(
                "<body xmlns='http://www.w3.org/1999/xhtml'>"
                + html
                + "</body>"
            )
        )
        msg.addChild(node=xhtml)
        xbot = xmpp.Node(
            "x",
            {
                "xmlns": "nwschat:nwsbot",
                "channels": ",".join(xtra["channels"]),
                "twitter_media": xtra["twitter_media"],
                "twitter": xtra["twitter"],
            },
        )
        # To prevent a bomb against iembot, we need to seed the cache by
        # requesting these twitter_media URLs
        try:
            LOG.info("Fetching %s", xtra["twitter_media"])
            # 30 seconds found to be not quite enough
            requests.get(xtra["twitter_media"], timeout=45)
        except Exception as exp:
            print(exp)
        msg.addChild(node=xbot)
        conn.send(msg)


def main():
    """Go Main Go."""
    # Load up the most recently processed data.
    current = pd.read_sql(
        "SELECT day, "
        "max(product_issue at time zone 'UTC') as last_product_issue from "
        "spc_outlook where outlook_type = 'E' "
        "and product_issue > now() - '7 days'::interval "
        "GROUP by day ORDER by day ASC",
        get_dbconnstr("postgis"),
        index_col="day",
    )
    # Loop over Days 1-3, eventually 4 and 5
    for day in range(1, 4):
        last_issue = utc(1980)  # default ancient
        if day in current.index:
            # Naive, convert to aware
            last_issue = current.at[day, "last_product_issue"].replace(
                tzinfo=timezone.utc
            )
        # Fetch ERO
        gdf, meta = fetch_ero(day)
        if gdf is None:
            continue
        # Compare with what we have, abort if same or older
        maxissue = meta["ISSUE_TIME"]
        if last_issue >= maxissue:
            LOG.info("%s is old %s >= %s", day, last_issue, maxissue)
            continue
        cycle = compute_cycle(day, maxissue)
        # Save to Database
        with get_dbconn("postgis") as dbconn:
            cursor = dbconn.cursor()
            save_df(cursor, gdf, meta, day, cycle)
            cursor.close()
        # Resave to GeoJson without pretty print and decreased precision.
        send_to_ldm(gdf, meta, maxissue, day, cycle)
        # Send Jabber Messages
        send_jabber(gdf, maxissue, day)


if __name__ == "__main__":
    main()
