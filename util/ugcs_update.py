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
from datetime import datetime, timezone

import click
import geopandas as gpd
import httpx
import pandas as pd
from psycopg.rows import dict_row
from pyiem.database import get_sqlalchemy_conn, sql_helper
from pyiem.util import logger
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
        table = f"gpw{year}"
        col = f"gpw_population_{year}"
        res = cursor.execute(
            sql_helper(
                """
    with pops as (
        select gid, coalesce(sum(population), 0) as pop
        from ugcs u LEFT JOIN {table} g on st_contains(u.geom, g.geom) where
        u.{col} is null GROUP by gid)
    update ugcs u SET {col} = pop FROM pops p WHERE u.gid = p.gid""",
                table=table,
                col=col,
            )
        )
        LOG.info("Set %s rows for gpw_population_%s", res.rowcount, year)

    res = cursor.execute(
        sql_helper("""
    update ugcs SET geom = st_makevalid(geom) where end_ts is null
    and not st_isvalid(geom) and begin_ts = :begin"""),
        {"begin": valid},
    )
    LOG.info("Fixed %s entries that were ST_Invalid()", res.rowcount)

    res = cursor.execute(
        sql_helper("""
        UPDATE ugcs SET simple_geom = ST_Multi(
            ST_Buffer(ST_SnapToGrid(geom, 0.01), 0)
        ),
        centroid = ST_Centroid(geom),
        area2163 = ST_area( ST_transform(geom, 2163) ) / 1000000.0
        WHERE begin_ts = :valid or area2163 is null
    """),
        {
            "valid": valid,
        },
    )
    LOG.info("Updated simple_geom,centroid,area2163 for %s rows", res.rowcount)

    # Check the last step that we don't have empty geoms, which happened once
    def _check():
        """Do the check."""
        return cursor.execute(
            sql_helper("""
            SELECT end_ts from ugcs
            where begin_ts = :valid and (
                simple_geom is null or
                ST_IsEmpty(simple_geom) or
                ST_Area(simple_geom) / ST_Area(geom) < 0.9
            )
        """),
            {
                "valid": valid,
            },
        )

    res = _check()
    if res.rowcount > 0:
        LOG.info(
            "%s rows with empty, too small simple_geom, decreasing tolerance",
            res.rowcount,
        )
        res = cursor.execute(
            sql_helper("""
            UPDATE ugcs
            SET simple_geom = ST_Multi(
                ST_Buffer(ST_SnapToGrid(geom, 0.0001), 0)
            )
            WHERE begin_ts = :valid and (
                simple_geom is null or
                ST_IsEmpty(simple_geom) or
                ST_Area(simple_geom) / ST_Area(geom) < 0.9
            )
        """),
            {
                "valid": valid,
            },
        )
        res = _check()
        if res.rowcount > 0:
            LOG.info(
                "Found %s rows with empty simple_geom, FIXME SOMEHOW!",
                res.rowcount,
            )


def truncate(cursor, valid, ugc, source, ctid):
    """Stop the bleeding."""
    res = cursor.execute(
        sql_helper("""
    UPDATE ugcs SET end_ts = :valid WHERE ugc = :ugc and end_ts is null
    and source = :source and ctid = :ctid"""),
        {
            "valid": valid,
            "ugc": ugc,
            "source": source,
            "ctid": ctid,
        },
    )
    return res.rowcount


def read_shapefile(shpfn: str) -> pd.DataFrame:
    """Standardized stuff."""
    df = gpd.read_file(shpfn)
    # Ensure CRS is set
    df["geometry"] = df["geometry"].set_crs("EPSG:4326", allow_override=True)
    if df.empty:
        LOG.info("Abort, empty dataframe from shapefile read.")
        sys.exit()
    # make all columns upper
    df.columns = [x.upper() if x != "geometry" else x for x in df.columns]
    return df


def chunk_df(df: pd.DataFrame):
    for ugc, gdfin in df.groupby("ugc"):
        gdf = gdfin.reset_index()
        if len(gdf.index) == 1:
            yield gdf.iloc[0].to_dict()
            continue
        LOG.info("Found %s rows for %s", len(gdf.index), ugc)
        # If name and cwa are the same, we can just merge the polygons
        if gdf["NAME"].nunique() == 1 and gdf["wfo"].nunique() == 1:
            LOG.info("--> Merging geometry of %s", ugc)
            yield {
                "ugc": ugc,
                "geometry": new_poly(gdf.union_all()),  # type: ignore
                "area2163": gdf["area2163"].sum(),
                "NAME": gdf.iloc[0]["NAME"],
                "STATE": gdf.iloc[0]["STATE"],
                "wfo": gdf.iloc[0]["wfo"],
            }
            continue
        LOG.info("--> Keeping all rows %s", ugc)
        for row in gdf.to_dict("records"):
            yield row


def workflow(pgconn, dt, filename):
    """Go Main Go"""
    zipfn = f"{filename}.zip"
    shpfn = do_download(zipfn)
    # track domain
    source = zipfn[:2].replace("_", "")
    LOG.info("Processing, using '%s' as the database source", source)
    df = read_shapefile(shpfn)
    # Compute the area and then sort to order duplicated UGCs :/
    # Database stores as sq km
    df["area2163"] = df["geometry"].to_crs(2163).area / 1e6
    df = df.sort_values(by="area2163", ascending=False)
    # Compute the ugc column
    wfocol = "CWA"
    if source in ["mz", "oz", "hz"]:
        df["STATE"] = ""
        df["ugc"] = df["ID"]
        wfocol = "WFO"
    elif source == "c":
        geo_type = "C"
        df["ugc"] = df["STATE"] + geo_type + df["FIPS"].str.slice(-3)
        df["NAME"] = df["COUNTYNAME"]
    else:
        geo_type = "Z"
        df["ugc"] = df["STATE"] + geo_type + df["ZONE"]
    df = df.rename(columns={wfocol: "wfo"})
    # Check that UGCs are not all null
    if df["ugc"].isna().all():
        LOG.info("Abort as all ugcs are null")
        sys.exit()
    postgis = gpd.read_postgis(
        sql_helper(
            "SELECT *, ctid from ugcs "
            "where end_ts is null and source = :source"
        ),
        pgconn,
        params={"source": source},
        geom_col="geom",
        index_col=None,  # We have dups at the UGC level
    )
    postgis["covered"] = False
    LOG.info(
        "Loaded %s '%s' type rows from the database",
        len(postgis.index),
        source,
    )
    # Rectify WFO
    df["wfo"] = df["wfo"].apply(lambda x: WFO_XREF.get(x, x))

    counts = {"new": 0, "changed": 0, "has": 0}
    # We need to dedup, but not in all cases, so alas
    for row in chunk_df(df):
        ugc = row["ugc"]
        name = row["NAME"].strip().replace("&", " and ")
        newugc = ugc not in postgis["ugc"].values
        current = postgis[
            (postgis["ugc"] == ugc)
            & (postgis["wfo"] == row["wfo"])
            & (postgis["name"] == name)
        ]
        # This should never happen, but just in case.
        if len(current.index) > 1:
            LOG.warning("FATAL ERROR, found multiple rows for %s", ugc)
            print(current)
            sys.exit()
        if len(current.index) == 1:
            # Do a crude area check
            if abs(row["area2163"] - current.iloc[0]["area2163"]) < 0.2:
                postgis.at[current.index[0], "covered"] = True
                counts["has"] += 1
                continue
            LOG.info(
                "%s updating sz diff %.2d -> %.2d",
                ugc,
                current.iloc[0]["area2163"],
                row["area2163"],
            )
            counts["changed"] += 1
        else:
            counts["new"] += 1

        LOG.info(
            "%s creating entry for %s[wfo=%s,isnew=%s]",
            ugc,
            name,
            row["wfo"],
            newugc,
        )

        # Finally, insert the new geometry
        pgconn.execute(
            sql_helper(
                "INSERT into ugcs (ugc, name, state, begin_ts, wfo, geom, "
                "source) VALUES (:ugc, :name, :state, :begints, :wfo, "
                "ST_Multi(ST_SetSRID(ST_GeomFromEWKT(:geom),4326)), :source)"
            ),
            {
                "ugc": ugc,
                "name": name,
                "state": row["STATE"],
                "begints": "1980-01-01" if newugc else dt,
                "wfo": row["wfo"],
                "geom": new_poly(row["geometry"]).wkt,
                "source": source,
            },
        )
    for _idx, row in postgis[~postgis["covered"]].iterrows():
        LOG.info("%s not found in update, truncating.", row["ugc"])
        truncate(pgconn, dt, row["ugc"], source, row["ctid"])

    LOG.info(
        "NEW: %s Updated: %s Has: %s",
        counts["new"],
        counts["changed"],
        counts["has"],
    )

    db_fixes(pgconn, dt)


@click.command()
@click.option(
    "--date",
    "dt",
    required=True,
    type=click.DateTime(),
    help="Date of the data",
)
@click.option("--filename", required=True, help="Zip file name (no extension)")
@click.option("--dryrun", is_flag=True, help="Dry run")
def main(dt: datetime, filename: str, dryrun: bool) -> None:
    """Go Main Go"""
    # Assumption is 18 UTC implementation timestamp
    dt = dt.replace(hour=18, tzinfo=timezone.utc)
    with get_sqlalchemy_conn("postgis") as pgconn:
        pgconn.row_factory = dict_row
        workflow(pgconn, dt, filename)
        if not dryrun:
            pgconn.commit()
        else:
            LOG.warning("---- DRY RUN, no commit ----")
    LOG.info("Done!")


if __name__ == "__main__":
    main()
