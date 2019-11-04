"""Process and Archive the NEXRAD Level III NCR Attribute Table"""
from io import BytesIO
import math

import pytz
from twisted.python import log
from twisted.internet import reactor
from metpy.io.nexrad import Level3File
from pyldm import ldmbridge
import common

# Setup Database Links
PGCONN = common.get_database("radar")

ST = {}


def load_station_table(txn):
    """ Load the station table of NEXRAD sites """
    log.msg("load_station_table called() ...")
    txn.execute(
        """
        SELECT id, ST_x(geom) as lon, ST_y(geom) as lat from stations
        where network in ('NEXRAD','TWDR')
    """
    )
    for row in txn.fetchall():
        ST[row["id"]] = {"lat": row["lat"], "lon": row["lon"]}
    log.msg("Station Table size %s" % (len(ST.keys())))


class MyProductIngestor(ldmbridge.LDMProductReceiver):
    """My ingest protocol"""

    def connectionLost(self, reason):
        """Called when stdin is closed"""
        log.msg("connectionLost")
        log.err(reason)
        reactor.callLater(15, reactor.callWhenRunning, reactor.stop)

    def process_data(self, data):
        """I am called from the ldmbridge when data is ahoy"""
        bio = BytesIO()
        bio.write(data)
        bio.seek(0)
        process(bio)


def process(bio):
    """Process our data, please"""
    l3 = Level3File(bio)
    del bio
    ctx = {}
    ctx["nexrad"] = l3.siteID
    ctx["ts"] = l3.metadata["vol_time"].replace(tzinfo=pytz.UTC)
    ctx["lines"] = []
    if not hasattr(l3, "graph_pages"):
        log.msg("%s %s has no graph_pages" % (ctx["nexrad"], ctx["ts"]))
        return
    for page in l3.graph_pages:
        for line in page:
            if "text" in line:
                ctx["lines"].append(line["text"])
    df = PGCONN.runInteraction(really_process, ctx)
    df.addErrback(common.email_error, ctx)


def delete_prev_attrs(txn, nexrad):
    """Remove any previous attributes for this nexrad"""
    txn.execute("DELETE from nexrad_attributes WHERE nexrad = %s", (nexrad,))


def really_process(txn, ctx):
    """
    This processes the output we get from the GEMPAK!

      STM ID  AZ/RAN TVS  MDA  POSH/POH/MX SIZE VIL DBZM  HT  TOP  FCST MVMT
         F0  108/ 76 NONE NONE    0/ 20/<0.50    16  53  8.3  16.2    NEW
         G0  115/ 88 NONE NONE    0/  0/ 0.00     8  47 10.2  19.3    NEW
         H0  322/ 98 NONE NONE    0/  0/ 0.00     6  43 12.1  22.2    NEW

      STM ID  AZ/RAN TVS  MDA  POSH/POH/MX SIZE VIL DBZM  HT  TOP  FCST MVMT
         H1  143/ 90 NONE NONE    0/  0/ 0.00     4  43 11.3  14.9  208/ 27
         Q5  125/ 66 NONE NONE    0/  0/ 0.00     3  42  9.3   9.3    NEW
         I8  154/ 73 NONE NONE    0/  0/ 0.00     1  33 10.9  10.9    NEW

         U8  154/126 NONE NONE     UNKNOWN       11  47 18.8  23.1  271/ 70
         J0  127/134 NONE NONE     UNKNOWN       24  51 20.2  33.9    NEW
    """
    delete_prev_attrs(txn, ctx["nexrad"])

    cenlat = float(ST[ctx["nexrad"]]["lat"])
    cenlon = float(ST[ctx["nexrad"]]["lon"])
    latscale = 111137.0
    lonscale = 111137.0 * math.cos(cenlat * math.pi / 180.0)

    #   STM ID  AZ/RAN TVS  MESO POSH/POH/MX SIZE VIL DBZM  HT  TOP  FCST MVMT
    co = 0
    for line in ctx["lines"]:
        if len(line) < 5:
            continue
        if line[1] != " ":
            continue
        tokens = line.replace(">", " ").replace("/", " ").split()
        if not tokens or tokens[0] == "STM":
            continue
        if tokens[5] == "UNKNOWN":
            tokens[5] = 0
            tokens.insert(5, 0)
            tokens.insert(5, 0)
        if len(tokens) < 13:
            log.msg("Incomplete Line ||%s||" % (line,))
            continue
        d = {}
        co += 1
        d["storm_id"] = tokens[0]
        d["azimuth"] = float(tokens[1])
        if tokens[2] == "***":
            log.msg("skipping bad line |%s|" % (line,))
            continue
        d["range"] = float(tokens[2]) * 1.852
        d["tvs"] = tokens[3]
        d["meso"] = tokens[4]
        d["posh"] = tokens[5]
        d["poh"] = tokens[6] if tokens[6] != "***" else None
        if tokens[7] == "<0.50":
            tokens[7] = 0.01
        d["max_size"] = tokens[7]

        if tokens[8] == "UNKNOWN":
            d["vil"] = 0
        else:
            d["vil"] = tokens[8]

        d["max_dbz"] = tokens[9]
        d["max_dbz_height"] = tokens[10]
        d["top"] = tokens[11]
        if tokens[12] == "NEW":
            d["drct"] = 0
            d["sknt"] = 0
        else:
            d["drct"] = int(float(tokens[12]))
            d["sknt"] = tokens[13]
        d["nexrad"] = ctx["nexrad"]

        cosaz = math.cos(d["azimuth"] * math.pi / 180.0)
        sinaz = math.sin(d["azimuth"] * math.pi / 180.0)
        mylat = cenlat + (cosaz * (d["range"] * 1000.0) / latscale)
        mylon = cenlon + (sinaz * (d["range"] * 1000.0) / lonscale)
        d["geom"] = "SRID=4326;POINT(%s %s)" % (mylon, mylat)
        d["valid"] = ctx["ts"]

        for table in [
            "nexrad_attributes",
            "nexrad_attributes_%s" % (ctx["ts"].year,),
        ]:
            sql = (
                """
                INSERT into """
                + table
                + """
                (nexrad, storm_id, geom, azimuth,
                range, tvs, meso, posh, poh, max_size, vil, max_dbz,
                max_dbz_height, top, drct, sknt, valid)
                values (%(nexrad)s, %(storm_id)s, ST_GeomFromEWKT(%(geom)s),
                %(azimuth)s, %(range)s, %(tvs)s, %(meso)s, %(posh)s,
                %(poh)s, %(max_size)s, %(vil)s, %(max_dbz)s,
                %(max_dbz_height)s, %(top)s, %(drct)s, %(sknt)s, %(valid)s)
            """
            )
            txn.execute(sql, d)

    if co > 0:
        log.msg(
            ("%s %s Processed %s entries")
            % (ctx["nexrad"], ctx["ts"].strftime("%Y-%m-%d %H:%M UTC"), co)
        )


def on_ready(_unused, mesosite):
    """ready to fire things up"""
    log.msg("on_ready() has fired...")
    mesosite.close()
    ingest = MyProductIngestor(isbinary=True)
    ldmbridge.LDMProductFactory(ingest)


def errback(res):
    """ ERRORBACK """
    log.err(res)
    reactor.stop()


def main():
    """Go Main Go"""
    mesosite = common.get_database("mesosite", cp_max=1)
    df = mesosite.runInteraction(load_station_table)
    df.addCallback(on_ready, mesosite)
    df.addErrback(errback)
    reactor.run()


if __name__ == "__main__":
    main()
