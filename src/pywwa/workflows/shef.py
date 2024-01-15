"""SHEF product ingestor."""
# stdlib
import datetime
import random
import re
from collections import namedtuple
from typing import List
from zoneinfo import ZoneInfo

# 3rd Party
# pylint: disable=no-name-in-module
import click
from psycopg.errors import DeadlockDetected
from pyiem.models.shef import SHEFElement
from pyiem.nws.products.shef import parser
from pyiem.observation import Observation
from pyiem.util import convert_value, utc
from twisted.internet import reactor
from twisted.internet.task import LoopingCall, deferLater

# Local
from pywwa import CTX, LOG, common
from pywwa.database import get_database, get_dbconnc
from pywwa.ldm import bridge

# A list of AFOS IDs that we will exclude
AFOS_EXCLUDE = []

# a form for IDs we will log as unknown
NWSLIRE = re.compile("^[A-Z]{4}[0-9]$")

# AFOS IDs for which we do not save the raw report for
SKIP4REPORT = re.compile("^(HYD|RTP)")

# stations we don't know about
UNKNOWN = {}
# station metadata
LOCS = {}
# a queue for saving database IO
CURRENT_QUEUE = {}
# Data structure to hold potential writes to IEMAccess
# [iemid] -> ACCESSDB_ENTRY -> records -> localts -> {}
ACCESSDB_QUEUE = {}
ACCESSDB_ENTRY = namedtuple(
    "ACCESSDB_ENTRY",
    [
        "station",
        "network",
        "tzinfo",
        "records",
    ],
)
U1980 = utc(1980)
# Networks that can come via SHEF backdoors
DOUBLEBACKED_NETWORKS = ["ISUSM", "IA_RWIS"]
P1H = datetime.timedelta(hours=1)
P60D = datetime.timedelta(days=60)


DIRECTMAP = {
    "HGIZ": "rstage",
    "HPIZ": "rstage",
    "HTIZ": "rstage",
    "PAIZ": "pres",
    "PPHZ": "phour",
    "PPDZ": "pday",
    "PPPZ": "pday",
    "PCIZ": "pcounter",
    "QRIZ": "discharge",
    "QTIZ": "discharge",
    "RWIZ": "srad",
    "SDIZ": "snowd",
    "SFDZ": "snow",
    "SWIZ": "snoww",
    "TDIZ": "dwpf",
    "TAIZ": "tmpf",
    "TAIN": "min_tmpf",
    "TAIX": "max_tmpf",
    "TWIZ": "water_tmpf",
    "UDIZ": "drct",
    "UGIZ": "gust",
    "UPIZ": "gust",
    "UPHZ": "gust",
    "URHZ": "max_drct",
    "URIZ": "max_drct",
    "USIZ": "sknt",
    "VBIZ": "battery",
    "XRIZ": "relh",
    "XVIZ": "vsby",
}


class MyDict(dict):
    """
    Customized dictionary class
    """

    def __getitem__(self, key):
        """
        Over-ride getitem so that the sql generated is proper
        """
        val = self.get(key)
        if val is not None:
            return val
        # Logic to convert this key into something the iem can handle
        # Our key is always at least 6 chars!
        pei = f"{key[:3]}{key[5]}"
        if pei in DIRECTMAP:
            self.__setitem__(key, DIRECTMAP[pei])
            return DIRECTMAP[pei]
        LOG.info("Can not map var %s", key)
        self.__setitem__(key, "")
        return ""


MAPPING = MyDict()


def load_stations_fe(blocking=False):
    """Load station metadata, sync is if we use defers."""
    if blocking:
        conn, cursor = get_dbconnc("mesosite")
        load_stations(cursor)
        cursor.close()
        conn.close()
    else:
        dbpool = get_database("mesosite", cp_max=1)
        df = dbpool.runInteraction(load_stations)
        df.addErrback(common.email_error)
        df.addBoth(lambda _: dbpool.close())


def load_stations(txn):
    """Load station metadata

    We need this information as we can't reliably convert a station ID found
    in a SHEF encoded product to a network that is necessary to update IEM
    Access.

    Args:
      txn: a database transaction
    """
    LOG.info("load_stations called...")
    # When tzname is null, we likely have an incomplete entry
    txn.execute(
        """
        SELECT id, s.iemid, network, tzname, a.value as pedts from stations s
        LEFT JOIN station_attributes a on (s.iemid = a.iemid and
        a.attr = 'PEDTS') WHERE (network ~* 'COOP'
        or network ~* 'DCP' or network = ANY(%s))
        and tzname is not null ORDER by network ASC
        """,
        (DOUBLEBACKED_NETWORKS,),
    )

    # A sentinel to know if we later need to remove things in the case of a
    # station that got dropped from a network
    epoc = utc().microsecond
    for row in txn.fetchall():
        stid = row["id"]
        iemid = row["iemid"]
        network = row["network"]
        try:
            tzinfo = ZoneInfo(row["tzname"])
        except Exception:
            LOG.info("ZoneInfo does not like tzname: %s", row["tzname"])
            tzinfo = ZoneInfo("UTC")
        pedts = row["pedts"]
        if stid in UNKNOWN:
            LOG.info("  station: %s is no longer unknown!", stid)
            UNKNOWN.pop(stid)
        metadata = LOCS.setdefault(stid, {})
        if network not in metadata:
            metadata[network] = {
                "valid": U1980,
                "iemid": iemid,
                "tzinfo": tzinfo,
                "epoc": epoc,
                "pedts": pedts,
            }
        else:
            metadata[network]["epoc"] = epoc

    # Now we find things that are outdated, note that other code can add
    # placeholders that can get zapped here.
    for stid in list(LOCS):
        for network in list(LOCS[stid]):
            if LOCS[stid][network]["epoc"] != epoc:
                LOG.info(" sid: %s no longer in network: %s", stid, network)
                LOCS[stid].pop(network)
        if not LOCS[stid]:
            LOG.info(" sid: %s removed completely", stid)
            LOCS.pop(stid)

    LOG.info("loaded %s stations", len(LOCS))


def restructure_data(prod):
    """Create a nicer data structure for future processing."""
    mydata = {}
    # Step 1: Restructure and do some cleaning
    # se == SHEFElement
    old = []
    utcnow = common.utcnow()
    for se in prod.data:
        # We don't want any non-report / forecast data, missing data
        if se.type != "R":
            continue
        # We don't care about data in the future!
        if se.valid > (utcnow + P1H):
            continue
        if se.valid < (utcnow - P60D):
            if se.station in old:
                continue
            LOG.info("Rejecting old data %s %s", se.station, se.valid)
            old.append(se.station)
            continue
        s_data = mydata.setdefault(se.station, {})
        st_data = s_data.setdefault(se.valid, [])

        # TODO handling of DV properly
        st_data.append(se)
    return mydata


def enter_unknown(cursor, sid, product_id, network):
    """
    Enter some info about a site ID we know nothing of...
    @param sid string site id
    @param product_id string
    @param network string of the guessed network
    """
    # Eh, lets not care about non-5 char IDs
    if NWSLIRE.match(sid) is None:
        return
    cursor.execute(
        "INSERT into unknown(nwsli, product, network) values (%s, %s, %s)",
        (sid, product_id, network),
    )


def checkvars(myvars):
    """
    Check variables to see if we have a COOP or DCP site
    """
    for v in myvars:
        # Definitely DCP
        if v[:2] in ["HG"]:
            return False
        if v[:2] in ["SF", "SD"]:
            return True
        if v[:3] in ["PPH"]:
            return False
    return False


def save_current() -> int:
    """update the database current_shef table

    It turns out that our database can't handle this fast enough in realtime,
    so we aggregate some
    """
    cnt = 0
    skipped = 0
    for k, mydict in CURRENT_QUEUE.items():
        if not mydict["dirty"]:
            skipped += 1
            continue
        cnt += 1
        # GH215 somehow sid has a | in it, maybe
        (sid, varname, _depth) = k.split("|", 2)
        # This is unlikely, but still GIGO sometimes :/
        if len(varname) != 7:
            LOG.info("Got varname of '%s' somehow? %s", varname, mydict)
            continue

        d2 = CTX["ACCESSDB"].runOperation(
            "INSERT into current_shef(station, valid, physical_code, "
            "duration, source, type, extremum, probability, value, depth, "
            "dv_interval, unit_convention, qualifier, product_id) "
            "values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
            (
                sid,
                mydict["valid"],
                varname[:2],
                varname[2],
                varname[3],
                varname[4],
                varname[5],
                varname[6],
                mydict["value"],
                mydict["depth"],
                mydict["dv_interval"],
                mydict["unit_convention"],
                mydict["qualifier"],
                mydict["product_id"],
            ),
        )
        d2.addErrback(common.email_error, "")
        mydict["dirty"] = False

    LOG.info("processed %s entries, %s skipped", cnt, skipped)
    return cnt


def get_localtime(sid, ts):
    """Compute the local timestamp for this location"""
    if sid not in LOCS:
        return ts
    _network = list(LOCS[sid])[0]
    return ts.astimezone(LOCS[sid][_network]["tzinfo"])


def get_network(prod, sid, data: List[SHEFElement]) -> str:
    """Logic for figuring out network in face of ambiguity.

    Note: This sid should already be in LOCS, so we are only either taking
    the single entry or picking between DCP and COOP variants.
    """
    networks = list(LOCS[sid].keys())
    # This is the best we can hope for
    if len(networks) == 1:
        return networks[0]
    # Our simple determination if the site is a COOP site
    is_coop = False
    varnames = [se.varname() for se in data]
    if prod.afos[:3] == "RR3":
        is_coop = True
    elif prod.afos[:3] in ["RR1", "RR2"] and checkvars(varnames):
        LOG.info(
            "Guessing COOP? %s %s %s", sid, prod.get_product_id(), varnames
        )
        is_coop = True
    pnetwork = "COOP" if is_coop else "DCP"
    # filter networks now
    for network in networks:
        if network.find(pnetwork) > 0:
            return network
    # Throw our hands up in the air
    return networks[0]


def process_site(prod, sid, data):
    """Consumption of rectified data."""
    # Order the timestamps so that we process the newest data last, so that
    # all obs are potentially processed through iemaccess
    times = list(data.keys())
    times.sort()
    for tstamp in times:
        process_site_time(prod, sid, tstamp, data[tstamp])


def insert_raw_inbound(cursor, args) -> int:
    """Do the database insertion."""
    cursor.execute(
        "INSERT into raw_inbound (station, valid, key, value, depth, "
        "unit_convention, dv_interval, qualifier) "
        "VALUES(%s, %s, %s, %s, %s, %s, %s, %s)",
        args,
    )
    return cursor.rowcount


def update_current_queue(element: SHEFElement, product_id: str):
    """Update CURRENT_QUEUE with new data."""
    # We only want observations
    if element.type != "R":
        return
    args = (
        element.station,
        element.valid,
        element.varname(),
        element.num_value,
        element.depth,
        element.unit_convention,
        element.dv_interval,
        element.qualifier,
    )
    defer = CTX["HADSDB"].runInteraction(insert_raw_inbound, args)
    defer.addErrback(common.email_error, f"prod: {product_id}")
    defer.addErrback(LOG.error)

    key = f"{element.station}|{element.varname()}|{element.depth}"
    cur = CURRENT_QUEUE.setdefault(
        key, dict(valid=element.valid, value=element.num_value, dirty=True)
    )
    if element.valid < cur["valid"]:
        return
    cur["valid"] = element.valid
    cur["depth"] = element.depth
    cur["value"] = element.num_value
    cur["dv_interval"] = element.dv_interval
    cur["qualifier"] = element.qualifier
    cur["unit_convention"] = element.unit_convention
    cur["product_id"] = product_id
    cur["dirty"] = True


def process_site_time(prod, sid, ts, elements: List[SHEFElement]):
    """Ingest for IEMAccess."""
    network = get_network(prod, sid, elements)
    if network is None or network in DOUBLEBACKED_NETWORKS:
        return
    localts = get_localtime(sid, ts)
    # This should always work!
    metadata = LOCS[sid][network]
    # Do not send DCP sites with old data to IEMAccess
    if network.find("_DCP") > 0 and localts < metadata["valid"]:
        return
    metadata["valid"] = localts

    # Okay, time for a hack, if our observation is at midnight!
    if localts.hour == 0 and localts.minute == 0:
        localts -= datetime.timedelta(minutes=1)

    record = ACCESSDB_QUEUE.setdefault(
        metadata["iemid"],
        ACCESSDB_ENTRY(
            station=sid,
            network=network,
            tzinfo=metadata["tzinfo"],
            records={},
        ),
    ).records.setdefault(
        localts,
        {
            "last": U1980,
            "data": {},
            "product_id": prod.get_product_id(),
        },
    )
    # Update last
    record["last"] = utc()

    report = None
    afos = prod.afos
    for se in elements:
        if se.narrative:
            report = se.narrative
        if (
            report is None
            and afos is not None
            and SKIP4REPORT.match(afos[:3]) is None
        ):
            report = se.raw
        varname = se.varname()
        iemvar = MAPPING[varname]
        if iemvar == "":  # or (iemvar == "rstage" and pedts is not None):
            continue
        # We generally are in english units, this also converts the wind
        # direction to the full degrees
        val = se.to_english()
        if val is None:
            # Behold, glorious hack here to force nulls into the summary
            # database that uses coerce
            record["data"][f"null_{iemvar}"] = None
        record["data"][iemvar] = val
        if iemvar in ["pday", "snow", "snowd"]:
            # Prevent negative numbers
            if val is not None and val < 0:
                record["data"][iemvar] = 0
        if iemvar in ["sknt", "gust"] and val is not None:
            # mph to knots :/
            val = convert_value(val, "mile / hour", "knot")
        if network.find("_COOP") > 0:
            # Save COOP 'at-ob' temperature into summary table
            if iemvar == "tmpf":
                record["data"]["coop_tmpf"] = val
            # Save observation time into the summary table
            if iemvar in [
                "tmpf",
                "max_tmpf",
                "min_tmpf",
                "pday",
                "snow",
                "snowd",
            ]:
                record["data"]["coop_valid"] = localts

    record["data"]["raw"] = prod.get_product_id()
    record["data"]["report"] = report


def write_access_records(accesstxn, records: [], iemid, entry: ACCESSDB_ENTRY):
    """Batch the records to to prevent deadlocks, maybe!"""
    for localts, record in records:
        write_access_record(accesstxn, record, iemid, localts, entry)


def write_access_record(
    accesstxn, record: dict, iemid, localts, entry: ACCESSDB_ENTRY
):
    """The batched database write."""
    iemob = Observation(iemid=iemid, valid=localts, tzname=entry.tzinfo.key)
    iemob.data.update(record["data"])
    iscoop = entry.network.find("_COOP") > 0
    iemob.save(accesstxn, force_current_log=iscoop)


def log_database_queue_size():
    """Log how much backlog has accumulated."""
    LOG.info(
        "dbpool queuesz[hads:%s, access:%s]",
        CTX["HADSDB"].threadpool._queue.qsize(),  # skipcq: PYL-W0212
        CTX["ACCESSDB"].threadpool._queue.qsize(),  # skipcq: PYL-W0212
    )


def write_access_records_eb(err, records: list, iemid, entry: ACCESSDB_ENTRY):
    """Errorback from process_site transaction."""
    if isinstance(err.value, DeadlockDetected):
        jitter = random.randint(0, 30)
        LOG.info(
            "Database Deadlock: %s[%s], retrying in %s seconds",
            entry.station,
            entry.network,
            jitter,
        )
        df = deferLater(
            reactor,
            jitter,
            CTX["ACCESSDB"].runInteraction,
            write_access_records,
            records,
            iemid,
            entry,
        )
        df.addErrback(common.email_error)
        return
    common.email_error(err, f"write_access_entry({entry.station}) got {err}")


def process_data(text):
    """Callback when text is received."""
    prod = parser(text, utcnow=common.utcnow())
    if prod.warnings:
        common.email_error("\n".join(prod.warnings), prod.unixtext)
    if prod.afos in AFOS_EXCLUDE:
        LOG.info("Skipping AFOS: %s due to in AFOS_EXCLUDE", prod.afos)
        prod.data = []
        return prod
    if not prod.data:
        return prod
    product_id = prod.get_product_id()
    # Update CURRENT_QUEUE
    time_threshold = common.utcnow() + P1H
    for element in [e for e in prod.data if e.valid < time_threshold]:
        update_current_queue(element, product_id)
    # Create a nicer data structure
    mydata = restructure_data(prod)
    # Chunk thru each of the sites found and do work.
    for sid, data in mydata.items():
        # Can't send unknown sites to iemaccess
        if sid in UNKNOWN:
            continue
        if sid in LOCS:
            process_site(prod, sid, data)
        else:
            UNKNOWN[sid] = True
            if NWSLIRE.match(sid) is not None:
                LOG.info("Unknown NWSLIish site: %s %s", sid, product_id)
                CTX["HADSDB"].runInteraction(
                    enter_unknown, sid, product_id, ""
                )
    return prod


def process_accessdb_frontend():
    """Catch exceptions so that Looping call keeps going."""
    try:
        process_accessdb()
    except Exception as exp:
        LOG.exception(exp)


def process_accessdb():
    """Queue up work to do."""
    threshold = utc() - datetime.timedelta(minutes=5)
    for iemid, entry in ACCESSDB_QUEUE.items():
        records = []
        for localts in list(entry.records.keys()):
            # Allow a window of time to accumulate data prior to writing
            if entry.records[localts]["last"] > threshold:
                continue
            # Get a reference and delete it from the dict
            records.append((localts, entry.records.pop(localts)))
        if records:
            df = CTX["ACCESSDB"].runInteraction(
                write_access_records,
                records,
                iemid,
                entry,
            )
            df.addErrback(
                write_access_records_eb,
                records,
                iemid,
                entry,
            )
            df.addErrback(common.email_error)


def build_context():
    """Build up things necessary for this to run."""
    if "pywwa_shef_afos_exclude" in CTX:
        AFOS_EXCLUDE.extend(CTX["pywwa_shef_afos_exclude"].split(","))
        LOG.info("AFOS_EXCLUDE: %s", AFOS_EXCLUDE)
    load_stations_fe(True)

    # Construct the needed database pools
    CTX["ACCESSDB"] = get_database("iem", module_name="psycopg", cp_max=20)
    CTX["HADSDB"] = get_database("hads", module_name="psycopg", cp_max=20)

    # Add some looping calls
    CTX["LCLIST"].extend(
        [
            # write out cached obs to IEMAccess
            LoopingCall(lc_proxy, save_current).start(131, now=False),
            # Run a logged diagnostic on how busy the database pools are
            LoopingCall(lc_proxy, log_database_queue_size).start(
                61, now=False
            ),
            # Reload the station table every 12 hours
            LoopingCall(lc_proxy, load_stations_fe).start(
                60 * 60 * 12, now=False
            ),
            # Process entries in ACCESSDB_QUEUE
            LoopingCall(lc_proxy, process_accessdb_frontend).start(
                15, now=False
            ),
        ]
    )


def lc_proxy(f):
    """Ensure that we don't have an uncaught exception."""
    try:
        f()
    except Exception as exp:
        common.email_error(exp, f.__name__)
        LOG.exception(exp)


@click.command(help=__doc__)
@click.option("--custom-arg", "-c", type=str, help="Differentiate pqact job")
@common.init
@common.disable_xmpp
def main(*args, **kwargs):
    """We startup."""
    build_context()
    bridge(process_data)
