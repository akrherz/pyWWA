"""
My purpose in life is to take the NWS AWIPS Geodata Zones Shapefile and
dump them into the PostGIS database!  I was bootstraped like so:

python ugcs_update.py z_16mr06 2006 03 16
python ugcs_update.py z_11mr07 2007 03 11
python ugcs_update.py z_31my07 2007 05 31
python ugcs_update.py z_01au07 2007 08 01
python ugcs_update.py z_5sep07 2007 09 05
python ugcs_update.py z_25sep07 2007 09 25
python ugcs_update.py z_01ap08 2008 04 01
python ugcs_update.py z_09se08 2008 09 09
python ugcs_update.py z_03oc08 2008 10 03
python ugcs_update.py z_07my09 2009 05 07
python ugcs_update.py z_15jl09 2009 07 15
python ugcs_update.py z_22jl09 2009 07 22
python ugcs_update.py z_04au11 2011 08 04
python ugcs_update.py z_13oc11 2011 10 13
python ugcs_update.py z_31my11 2011 05 31
python ugcs_update.py z_15de11 2011 12 15
python ugcs_update.py z_23fe12 2012 02 23
python ugcs_update.py z_03ap12 2012 04 03
python ugcs_update.py z_12ap12 2012 04 12
python ugcs_update.py z_07jn12 2012 06 07
python ugcs_update.py z_11oc12 2012 10 11
python ugcs_update.py z_03de13a 2013 12 03
python ugcs_update.py z_05fe14a 2014 02 05

"""

import os
import sys
import zipfile

import geopandas as gpd
import httpx
from pyiem.util import get_sqlalchemy_conn, logger, utc
from pywwa.database import get_dbconnc
from shapely.geometry import MultiPolygon

# Some strangely encoded WFOs need to be rectified
WFO_XREF = {
    "PQW": "GUM",
    "PQE": "GUM",
}

LOG = logger()
# Change Directory to /tmp, so that we can rw
os.chdir("/tmp")


def do_download(zipfn):
    """Do the download steps"""
    if not os.path.isfile(zipfn):
        url = (
            "https://www.weather.gov/source/gis/Shapefiles/"
            f"{'County' if zipfn.startswith('c_') else 'WSOM'}/{zipfn}"
        )
        req = httpx.get(url, timeout=60)
        LOG.info("Downloading %s ...", url)
        if req.status_code != 200:
            LOG.warning("Download %s failed, got %s", zipfn, req.status_code)
            sys.exit(1)
        with open(zipfn, "wb") as fh:
            fh.write(req.content)

    LOG.info("Unzipping")
    shpfn = None
    with zipfile.ZipFile(zipfn, "r") as zipfp:
        for name in zipfp.namelist():
            LOG.info("Extracting %s", name)
            with open(name, "wb") as fh:
                fh.write(zipfp.read(name))
            if name[-3:] == "shp":
                shpfn = name
    return shpfn


def new_poly(geo):
    """Sort and return new multipolygon"""
    if geo.geom_type == "Polygon":
        return geo
    # This is tricky. We want to have our multipolygon have its
    # biggest polygon first in the multipolygon.
    # This will allow some plotting simplification
    # later as we will only consider the first polygon
    maxarea = 0
    polys = []
    for poly in geo.geoms:
        area = poly.area
        if area > maxarea:
            maxarea = area
            polys.insert(0, poly)
        else:
            polys.append(poly)

    return MultiPolygon(polys)


def db_fixes(cursor, valid):
    """Fix some issues in the database"""
    for year in [2000, 2005, 2010, 2015, 2020]:
        cursor.execute(
            "with pops as (select gid, coalesce(sum(population), 0) as pop "
            f"from ugcs u LEFT JOIN gpw{year} g on "
            "st_contains(u.geom, g.geom) where "
            f"u.gpw_population_{year} is null GROUP by gid) "
            f"update ugcs u SET gpw_population_{year} = pop FROM pops p "
            "WHERE u.gid = p.gid"
        )
        LOG.info("Set %s rows for gpw_population_%s", cursor.rowcount, year)

    cursor.execute(
        "update ugcs SET geom = st_makevalid(geom) where end_ts is null "
        "and not st_isvalid(geom) and begin_ts = %s",
        (valid,),
    )
    LOG.info("Fixed %s entries that were ST_Invalid()", cursor.rowcount)

    cursor.execute(
        """
        UPDATE ugcs SET simple_geom = ST_Multi(
            ST_Buffer(ST_SnapToGrid(geom, 0.01), 0)
        ),
        centroid = ST_Centroid(geom),
        area2163 = ST_area( ST_transform(geom, 2163) ) / 1000000.0
        WHERE begin_ts = %s or area2163 is null
    """,
        (valid,),
    )
    LOG.info(
        "Updated simple_geom,centroid,area2163 for %s rows", cursor.rowcount
    )

    # Check the last step that we don't have empty geoms, which happened once
    def _check():
        """Do the check."""
        cursor.execute(
            """
            SELECT end_ts from ugcs
            where begin_ts = %s and (
                simple_geom is null or
                ST_IsEmpty(simple_geom) or
                ST_Area(simple_geom) / ST_Area(geom) < 0.9
            )
        """,
            (valid,),
        )

    _check()
    if cursor.rowcount > 0:
        LOG.info(
            "%s rows with empty, too small simple_geom, decreasing tolerance",
            cursor.rowcount,
        )
        cursor.execute(
            """
            UPDATE ugcs
            SET simple_geom = ST_Multi(
                ST_Buffer(ST_SnapToGrid(geom, 0.0001), 0)
            )
            WHERE begin_ts = %s and (
                simple_geom is null or
                ST_IsEmpty(simple_geom) or
                ST_Area(simple_geom) / ST_Area(geom) < 0.9
            )
        """,
            (valid,),
        )
        _check()
        if cursor.rowcount > 0:
            LOG.info(
                "Found %s rows with empty simple_geom, FIXME SOMEHOW!",
                cursor.rowcount,
            )


def truncate(cursor, valid, ugc, source):
    """Stop the bleeding."""
    cursor.execute(
        "UPDATE ugcs SET end_ts = %s WHERE ugc = %s and end_ts is null "
        "and source = %s",
        (valid, ugc, source),
    )
    return cursor.rowcount


def workflow(argv, cursor):
    """Go Main Go"""
    # NWS correspondence indicates the date on the website is assumed to be
    # an implementation time at 18 z of that date.
    valid = utc(int(argv[2]), int(argv[3]), int(argv[4]), 18)
    zipfn = f"{argv[1]}.zip"
    shpfn = do_download(zipfn)
    # track domain
    source = zipfn[:2].replace("_", "")
    LOG.info("Processing, using '%s' as the database source", source)

    df = gpd.read_file(shpfn)
    # Ensure CRS is set
    df["geometry"] = df["geometry"].set_crs("EPSG:4326", allow_override=True)
    if df.empty:
        LOG.info("Abort, empty dataframe from shapefile read.")
        sys.exit()
    # make all columns upper
    df.columns = [x.upper() if x != "geometry" else x for x in df.columns]
    # Compute the ugc column
    if zipfn[:2] in ("mz", "oz", "hz"):
        df["STATE"] = ""
        df["ugc"] = df["ID"]
        wfocol = "WFO"
    elif zipfn.startswith("c_"):
        geo_type = "C"
        df["ugc"] = df["STATE"] + geo_type + df["FIPS"].str.slice(-3)
        df["NAME"] = df["COUNTYNAME"]
        wfocol = "CWA"
    else:
        geo_type = "Z"
        df["ugc"] = df["STATE"] + geo_type + df["ZONE"]
        wfocol = "CWA"
    # Check that UGCs are not all null
    if df["ugc"].isna().all():
        LOG.info("Abort as all ugcs are null")
        sys.exit()
    with get_sqlalchemy_conn("postgis") as pgconn:
        postgis = gpd.read_postgis(
            "SELECT * from ugcs where end_ts is null and source = %s",
            pgconn,
            params=(source,),
            geom_col="geom",
            index_col="ugc",
        )
    postgis["covered"] = False
    LOG.info(
        "Loaded %s '%s' type rows from the database",
        len(postgis.index),
        source,
    )
    # Rectify WFO
    df[wfocol] = df[wfocol].apply(lambda x: WFO_XREF.get(x, x))

    # Compute the area and then sort to order duplicated UGCs :/
    # Database stores as sq km
    df["area2163"] = df["geometry"].to_crs(2163).area / 1e6
    df.sort_values(by="area2163", ascending=False, inplace=True)
    gdf = df.groupby("ugc").nth(0).set_index("ugc")
    LOG.info(
        "Loaded %s/%s unique entries from %s",
        len(gdf.index),
        len(df.index),
        shpfn,
    )
    countnew = 0
    countdups = 0
    for ugc, row in gdf.iterrows():
        name = row["NAME"].strip().replace("&", " and ")
        if ugc in postgis.index:
            postgis.at[ugc, "covered"] = True
            # Some very small number, good enough
            current = postgis.loc[ugc]
            if isinstance(current, gpd.GeoDataFrame):
                LOG.info("abort, more than one %s found in postgis", ugc)
                sys.exit()
            dirty = False
            # arb size decision
            if abs(row["area2163"] - current["area2163"]) > 0.2:
                dirty = True
                LOG.debug(
                    "%s updating sz diff %.2d -> %.2d",
                    ugc,
                    current["area2163"],
                    row["area2163"],
                )
            elif name != current["name"]:
                dirty = True
                LOG.debug(
                    "%s updating due to name change %s -> %s",
                    ugc,
                    current["name"],
                    name,
                )
            elif row[wfocol] != current["wfo"]:
                dirty = True
                LOG.debug(
                    "%s updating due to wfo change %s -> %s",
                    ugc,
                    current["wfo"],
                    row[wfocol],
                )
            if not dirty:
                countdups += 1
                continue

        res = truncate(cursor, valid, ugc, source)
        LOG.info(
            "%s creating new entry for %s",
            "Truncating old" if res > 0 else "",
            ugc,
        )

        # Finally, insert the new geometry
        cursor.execute(
            "INSERT into ugcs (ugc, name, state, begin_ts, wfo, geom, "
            "source) VALUES (%s, %s, %s, %s, %s, "
            "ST_Multi(ST_SetSRID(ST_GeomFromEWKT(%s),4326)), %s)",
            (
                ugc,
                name,
                row["STATE"],
                "1980-01-01" if res == 0 else valid,
                row[wfocol],
                new_poly(row["geometry"]).wkt,
                source,
            ),
        )
        countnew += 1

    for ugc, _row in postgis[~postgis["covered"]].iterrows():
        LOG.info("%s not found in update, truncating.", ugc)
        truncate(cursor, valid, ugc, source)

    LOG.info("NEW: %s Dups: %s", countnew, countdups)

    db_fixes(cursor, valid)


def main(argv):
    """Go Main Go"""
    if len(argv) != 5:
        LOG.info("ERROR: You need to specify the file date to download + date")
        LOG.info("Example:  python ugcs_update.py z_01dec10 2010 12 01")
        sys.exit(0)

    pgconn, cursor = get_dbconnc("postgis")
    workflow(argv, cursor)
    cursor.close()
    pgconn.commit()
    pgconn.close()
    LOG.info("Done!")


if __name__ == "__main__":
    # Get the name of the file we wish to download
    main(sys.argv)
