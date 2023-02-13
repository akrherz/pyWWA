"""SHEF product ingestor."""
# stdlib
import datetime
import random
import re
from typing import List

# 3rd Party
# pylint: disable=no-name-in-module
from psycopg2.errors import DeadlockDetected
import pytz
from twisted.internet import reactor
from twisted.internet.task import LoopingCall
from pyiem.observation import Observation
from pyiem.models.shef import SHEFElement
from pyiem.nws.products.shef import parser
from pyiem.util import LOG, utc, convert_value
from pyiem import reference

# Local
from pywwa import common
from pywwa.ldm import bridge
from pywwa.database import get_database

# Setup Database Links
# the current_shef table is not very safe when two processes attempt to update
# it at the same time, use a single process for this connection
ACCESSDB = get_database("iem", module_name="psycopg2", cp_max=20)
HADSDB = get_database("hads", module_name="psycopg2", cp_max=20)
MESOSITEDB = get_database("mesosite", cp_max=1)

# a form for IDs we will log as unknown
NWSLIRE = re.compile("[A-Z]{4}[0-9]")

# stations we don't know about
UNKNOWN = {}
# station metadata
LOCS = {}
# database timezones to pytz cache
TIMEZONES = {}
# a queue for saving database IO
CURRENT_QUEUE = {}
U1980 = utc(1980)


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


def load_stations(txn):
    """Load station metadata

    We need this information as we can't reliably convert a station ID found
    in a SHEF encoded product to a network that is necessary to update IEM
    Access.

    Args:
      txn: a database transaction
    """
    LOG.info("load_stations called...")
    txn.execute(
        """
        SELECT id, s.iemid, network, tzname, a.value as pedts from stations s
        LEFT JOIN station_attributes a on (s.iemid = a.iemid and
        a.attr = 'PEDTS') WHERE network ~* 'COOP'
        or network ~* 'DCP' or network = 'ISUSM' ORDER by network ASC
        """
    )

    # A sentinel to know if we later need to remove things in the case of a
    # station that got dropped from a network
    epoc = utc().microsecond
    for stid, iemid, network, tzname, pedts in txn.fetchall():
        if stid in UNKNOWN:
            LOG.info("  station: %s is no longer unknown!", stid)
            UNKNOWN.pop(stid)
        metadata = LOCS.setdefault(stid, {})
        if network not in metadata:
            metadata[network] = {
                "valid": U1980,
                "iemid": iemid,
                "tzname": tzname,
                "epoc": epoc,
                "pedts": pedts,
            }
        else:
            metadata[network]["epoc"] = epoc
        if tzname not in TIMEZONES:
            try:
                TIMEZONES[tzname] = pytz.timezone(tzname)
            except Exception:
                LOG.info("pytz does not like tzname: %s", tzname)
                TIMEZONES[tzname] = pytz.utc

    # Now we find things that are outdated, note that other code can add
    # placeholders that can get zapped here.
    for stid in list(LOCS):
        for network in list(LOCS[stid]):
            if LOCS[stid][network]["epoc"] != epoc:
                LOCS[stid].pop(network)
        if not LOCS[stid]:
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
        if len(se.station) > 8:
            LOG.info("sid len>8: [%s] %s", se.station, prod.get_product_id())
            continue
        # We don't want any non-report / forecast data, missing data
        if se.type != "R" or se.valid is None:
            continue
        # We don't care about data in the future!
        if se.valid > (utcnow + datetime.timedelta(hours=1)):
            continue
        if se.valid < (utcnow - datetime.timedelta(days=60)):
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


def enter_unknown(sid, product_id, network):
    """
    Enter some info about a site ID we know nothing of...
    @param sid string site id
    @param product_id string
    @param network string of the guessed network
    """
    # Eh, lets not care about non-5 char IDs
    if NWSLIRE.match(sid) is None:
        return
    HADSDB.runOperation(
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


def save_current():
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
        (sid, varname, _depth) = k.split("|")
        if len(varname) != 7:
            LOG.info("Got varname of '%s' somehow? %s", varname, mydict)
            continue

        d2 = ACCESSDB.runOperation(
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


def get_localtime(sid, ts):
    """Compute the local timestamp for this location"""
    if sid not in LOCS:
        return ts
    _network = list(LOCS[sid])[0]
    return ts.astimezone(
        TIMEZONES.get(LOCS[sid][_network]["tzname"], pytz.utc)
    )


def get_network(prod, sid, data: List[SHEFElement]):
    """Figure out which network this belongs to"""
    networks = list(LOCS.get(sid, {}).keys())
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
    networks = [s for s in networks if s.find(pnetwork) > 0]
    if len(networks) == 1:
        return networks[0]

    # If networks is zero length, then we have to try some things
    if not networks:
        enter_unknown(sid, prod.get_product_id(), "")
        if len(sid) == 5:
            state = reference.nwsli2state.get(sid[3:])
            country = reference.nwsli2country.get(sid[3:])
            if country in ["CA", "MX"]:
                return f"{country}_{state}_{pnetwork}"
            if country == "US":
                return f"{state}_{pnetwork}"
            return f"{country}__{pnetwork}"

    if sid not in UNKNOWN:
        UNKNOWN[sid] = True
        LOG.info("failure for sid: %s tp: %s", sid, prod.get_product_id())
    return None


def process_site_frontend(prod, sid, data):
    """Frontdoor for process_site()"""
    df = ACCESSDB.runInteraction(process_site, prod, sid, data)
    df.addErrback(process_site_eb, prod, sid, data)
    df.addErrback(common.email_error, prod.unixtext)
    df.addErrback(LOG.error)


def process_site(accesstxn, prod, sid, data):
    """Consumption of rectified data."""
    # Order the timestamps so that we process the newest data last, so that
    # all obs are potentially processed through iemaccess
    times = list(data.keys())
    times.sort()
    for tstamp in times:
        if sid in UNKNOWN:
            continue
        process_site_time(accesstxn, prod, sid, tstamp, data[tstamp])


def update_current_queue(element: SHEFElement, product_id: str):
    """Update CURRENT_QUEUE with new data."""
    # We only want observations
    if element.type != "R":
        return
    varname = element.varname()
    deffer = HADSDB.runOperation(
        "INSERT into raw_inbound (station, valid, key, value, depth, "
        "unit_convention, dv_interval, qualifier) "
        "VALUES(%s, %s, %s, %s, %s, %s, %s, %s)",
        (
            element.station,
            element.valid,
            varname,
            element.num_value,
            element.depth,
            element.unit_convention,
            element.dv_interval,
            element.qualifier,
        ),
    )
    deffer.addErrback(common.email_error, product_id)
    deffer.addErrback(LOG.error)

    key = f"{element.station}|{varname}|{element.depth}"
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


def process_site_time(accesstxn, prod, sid, ts, elements: List[SHEFElement]):
    """Ingest for IEMAccess."""
    network = get_network(prod, sid, elements)
    if network in ["ISUSM", None]:
        return

    localts = get_localtime(sid, ts)
    # Do not send DCP sites with old data to IEMAccess
    if network.find("_DCP") > 0 and localts < LOCS.get(sid, {}).get(
        "valid", localts
    ):
        return
    metadata = LOCS.setdefault(
        sid,
        {
            network: dict(
                valid=localts, iemid=-1, tzname=None, epoc=-1, pedts=None
            )
        },
    )
    metadata[network]["valid"] = localts

    # Okay, time for a hack, if our observation is at midnight!
    if localts.hour == 0 and localts.minute == 0:
        localts -= datetime.timedelta(minutes=1)
        # LOG.info("Shifting %s [%s] back one minute: %s" % (sid, network,
        #                                                  localts))

    iemob = Observation(
        iemid=metadata[network]["iemid"],
        valid=localts,
        tzname=metadata[network]["tzname"],
    )
    iscoop = network.find("COOP") > 0
    hasdata = False
    # TODO Special rstage logic in case PEDTS is defined
    # pedts = metadata[network]["pedts"]
    report = None
    for se in elements:
        if se.type != "R":
            continue
        if se.narrative:
            report = se.narrative
        varname = se.varname()
        iemvar = MAPPING[varname]
        if iemvar == "":  # or (iemvar == "rstage" and pedts is not None):
            continue
        # We generally are in english units, this also converts the wind
        # direction to the full degrees
        val = se.to_english()
        # TODO it is not clear if we can hit this code or not
        if val is None:
            # Behold, glorious hack here to force nulls into the summary
            # database that uses coerce
            iemob.data[f"null_{iemvar}"] = None
        hasdata = True
        iemob.data[iemvar] = val
        if iemvar in ["pday", "snow", "snowd"]:
            # Prevent negative numbers
            if val is not None and val < 0:
                iemob.data[iemvar] = 0
        if iemvar in ["sknt", "gust"] and val is not None:
            # mph to knots :/
            val = convert_value(val, "mile / hour", "knot")
        if iscoop:
            # Save COOP 'at-ob' temperature into summary table
            if iemvar == "tmpf":
                iemob.data["coop_tmpf"] = val
            # Save observation time into the summary table
            if iemvar in [
                "tmpf",
                "max_tmpf",
                "min_tmpf",
                "pday",
                "snow",
                "snowd",
            ]:
                iemob.data["coop_valid"] = iemob.data["valid"]
    # if pedts is not None and f"{pedts}Z" in data:
    #    val = None if data[f"{pedts}Z"] < -9998 else data[f"{pedts}Z"]
    #    iemob.data["rstage"] = val

    if not hasdata:
        return
    iemob.data["raw"] = prod.get_product_id()
    iemob.data["report"] = report
    # Only force COOP data into current_log even if we have newer obs
    if not iemob.save(accesstxn, force_current_log=iscoop):
        enter_unknown(sid, prod.get_product_id(), network)


def log_database_queue_size():
    """Log how much backlog has accumulated."""
    LOG.info(
        "dbpool queuesz[hads:%s, access:%s]",
        # pylint: disable=protected-access
        HADSDB.threadpool._queue.qsize(),
        ACCESSDB.threadpool._queue.qsize(),
    )


def process_site_eb(err, prod, sid, data):
    """Errorback from process_site transaction."""
    if isinstance(err.value, DeadlockDetected):
        jitter = random.randint(0, 30)
        LOG.info(
            "Database Deadlock: prod:%s sid:%s, retrying in %s seconds",
            prod.get_product_id(),
            sid,
            jitter,
        )
        reactor.callLater(jitter, process_site_frontend, prod, sid, data)
        return
    msg = f"process_site({prod.get_product_id()}, {sid}, {data}) got {err}"
    common.email_error(err, msg)


def process_data(text):
    """Callback when text is received."""
    prod = parser(text, utcnow=common.utcnow())
    if prod.warnings:
        common.email_error("\n".join(prod.warnings), prod.unixtext)
    if not prod.data:
        return prod
    product_id = prod.get_product_id()
    # Update CURRENT_QUEUE
    utcnow = common.utcnow()
    for element in prod.data:
        if element.valid > (utcnow + datetime.timedelta(hours=1)):
            continue
        update_current_queue(element, product_id)
    # Create a nicer data structure
    mydata = restructure_data(prod)
    # Chunk thru each of the sites found and do work.
    for sid, data in mydata.items():
        process_site_frontend(prod, sid, data)
    return prod


def main2(_res):
    """Go main Go!"""
    LOG.info("main() fired!")
    bridge(process_data)

    # Write out cached obs every couple of minutes
    lc = LoopingCall(save_current)
    df = lc.start(131, now=False)
    df.addErrback(common.email_error)
    # Log a diagnostic on how things are processing.
    lc2 = LoopingCall(log_database_queue_size)
    df2 = lc2.start(61, now=False)
    df2.addErrback(common.email_error)
    # Reload stations every 12 hours
    lc3 = LoopingCall(MESOSITEDB.runInteraction, load_stations)
    df3 = lc3.start(60 * 60 * 12, now=False)
    df3.addErrback(common.email_error)


def main():
    """We startup."""
    common.main(with_jabber=False)

    # Load the station metadata before we fire up the ingesting
    df = MESOSITEDB.runInteraction(load_stations)
    df.addCallback(main2)
    df.addErrback(common.shutdown)
    reactor.run()


if __name__ == "__main__":
    main()
