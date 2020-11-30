"""SHEF product ingestor."""
# stdlib
import os
import re
from collections import namedtuple
import datetime
from io import BytesIO

# 3rd Party
import pytz
from twisted.internet import task
from twisted.internet.defer import DeferredQueue, Deferred
from twisted.internet.task import cooperate
from twisted.internet import reactor, protocol
from twisted.internet.task import LoopingCall
from pyiem.reference import TRACE_VALUE
from pyiem.observation import Observation
from pyiem.nws import product
from pyiem.util import LOG
from pyiem import reference

# Local
from pywwa import common, get_search_paths
from pywwa.ldm import bridge
from pywwa.database import get_database

# from pympler import tracker, summary, muppy


# TR = tracker.SummaryTracker()

# Setup Database Links
# the current_shef table is not very safe when two processes attempt to update
# it at the same time, use a single process for this connection
ACCESSDB = get_database("iem", module_name="psycopg2", cp_max=20)
HADSDB = get_database("hads", module_name="psycopg2", cp_max=20)

# a form for IDs we will log as unknown
NWSLIRE = re.compile("[A-Z]{4}[0-9]")

# stations we don't know about
UNKNOWN = dict()
# station metadata
LOCS = dict()
# database timezones to pytz cache
TIMEZONES = dict()
# a queue for saving database IO
CURRENT_QUEUE = {}
U1980 = datetime.datetime.utcnow()
U1980 = U1980.replace(day=1, year=1980, tzinfo=pytz.utc)

TEXTPRODUCT = namedtuple("TextProduct", ["product_id", "afos", "text"])
JOBS = DeferredQueue()


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
        SELECT id, network, tzname from stations
        WHERE network ~* 'COOP' or network ~* 'DCP' or
        network in ('KCCI','KIMT','KELO', 'ISUSM')
        ORDER by network ASC
        """
    )

    LOCS.clear()  # clear out our current cache
    for (stid, network, tzname) in txn.fetchall():
        if stid in UNKNOWN:
            LOG.info("  station: %s is no longer unknown!", stid)
            UNKNOWN.pop(stid)
        if tzname is None or tzname == "":
            LOG.info("  station: %s has tzname: %s", stid, tzname)
        metadata = LOCS.setdefault(stid, dict())
        if network not in metadata:
            metadata[network] = dict(valid=U1980, tzname=tzname)
        if tzname not in TIMEZONES:
            try:
                TIMEZONES[tzname] = pytz.timezone(tzname)
            except Exception:
                LOG.info("pytz does not like tzname: %s", tzname)
                TIMEZONES[tzname] = pytz.utc

    LOG.info("loaded %s stations", len(LOCS))
    # Reload every 12 hours
    reactor.callLater(12 * 60 * 60, HADSDB.runInteraction, load_stations)


MULTIPLIER = {
    "US": 0.87,  # Convert MPH to KNT
    "UG": 0.87,
    "UP": 0.87,
    "UR": 10,
}

"""
Some notes on the SHEF codes translated to something IEM Access can handle
First two chars are physical extent code

"""
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
    "USIZ": "sknt",  # note above that we apply a multipler
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
        pei = "%s%s" % (key[:3], key[5])
        if pei in DIRECTMAP:
            self.__setitem__(key, DIRECTMAP[pei])
            return DIRECTMAP[pei]
        LOG.info("Can not map var %s", key)
        self.__setitem__(key, "")
        return ""


MAPPING = MyDict()


class SHEFIT(protocol.ProcessProtocol):
    """
    My process protocol for dealing with the SHEFIT program from the NWS
    """

    def __init__(self, prod):
        """
        Constructor
        """
        self.deferred = None
        self.prod = prod
        self.data = BytesIO()

    def connectionMade(self):
        """
        Fired when the program starts up and wants stdin
        """
        # prod.text is <str> need to write bytes
        self.transport.write(self.prod.text.encode("utf-8"))
        self.transport.closeStdin()

    def outReceived(self, data):
        """
        Save the stdout we get from the program for later processing
        """
        # data should come to us as <bytes>
        self.data.write(data)

    def errReceived(self, data):
        """
        In case something comes to stderr
        """
        LOG.info("errReceived! with %d bytes!", len(data))
        LOG.info(data)
        self.deferred.errback(data)

    #    def processEnded(self, status):
    #        print "debug: type(status): %s" % type(status.value)
    #        print "error: exitCode: %s" % status.value.exitCode

    def outConnectionLost(self):
        """
        Once the program is done, we need to do something with the data
        """
        df = task.deferLater(
            reactor,
            0,
            really_process,
            self.prod,
            self.data.getvalue().decode("utf-8"),
        )
        df.addErrback(common.email_error, self.prod.text)
        self.deferred.callback("")


def clnstr(buf):
    """
    Get rid of cruft we don't wish to work with
    """
    return (
        buf.replace("\015\015\012", "\n")
        .replace("\003", "")
        .replace("\001", "")
    )


def process_data(data):
    """callback when a full data product is ready for processing

    This string is cleaned and placed into the job queue for processing

    Args:
        data (str)
    """
    JOBS.put(clnstr(data))


def async_func(data):
    """spawn a process with a deferred given the inbound data product"""
    defer = Deferred()
    try:
        tp = product.TextProduct(data, parse_segments=False)
    except Exception as exp:
        common.email_error(exp, data)
        return None
    prod = TEXTPRODUCT(
        product_id=tp.get_product_id(), afos=tp.afos, text=tp.text
    )
    proc = SHEFIT(prod)
    proc.deferred = defer
    proc.deferred.addErrback(LOG.error)

    reactor.spawnProcess(proc, "./shefit", ["shefit"], {})
    return proc.deferred


def worker():
    """Our long running worker"""
    while True:
        yield JOBS.get().addCallback(async_func).addErrback(
            common.email_error, "Unhandled Error"
        ).addErrback(LOG.error)


def make_datetime(dpart, tpart):
    """Create a datatime instance from these two strings"""
    if dpart == "0000-00-00":
        return None
    dstr = "%s %s" % (dpart, tpart)
    tstamp = datetime.datetime.strptime(dstr, "%Y-%m-%d %H:%M:%S")
    return tstamp.replace(tzinfo=pytz.utc)


def really_process(prod, data):
    """
    This processes the output we get from the SHEFIT program
    """
    # Now we loop over the data we got :)
    # LOG.info("\n"+data)
    mydata = {}
    for line in data.split("\n"):
        # Skip blank output lines or short lines
        if line.strip() == "" or len(line) < 90:
            continue
        # data is fixed, so we should parse it
        sid = line[:8].strip()
        if len(sid) > 8:
            LOG.info("SiteID Len Error: [%s] %s", sid, prod.product_id)
            continue
        if sid not in mydata:
            mydata[sid] = {}
        modelruntime = make_datetime(line[31:41], line[42:50])
        if modelruntime is not None:
            # print("Skipping forecast data for %s" % (sid, ))
            continue
        tstamp = make_datetime(line[10:20], line[21:29])
        # We don't care about data in the future!
        utcnow = datetime.datetime.utcnow().replace(tzinfo=pytz.utc)
        if tstamp > (utcnow + datetime.timedelta(hours=1)):
            continue
        if tstamp < (utcnow - datetime.timedelta(days=60)):
            LOG.info("Rejecting old data %s %s", sid, tstamp)
            continue
        s_data = mydata.setdefault(sid, dict())
        st_data = s_data.setdefault(tstamp, dict())

        varname = line[52:59].strip()
        value = line[60:73].strip()
        if value.find("****") > -1:
            LOG.info("Bad Data from %s\n%s", prod.product_id, data)
            value = -9999.0
        else:
            value = float(value)
            # shefit generates 0.001 for trace, IEM uses something else
            if 0.0009 < value < 0.0011 and varname[:2] in [
                "PC",
                "PP",
                "QA",
                "QD",
                "QR",
                "QT",
                "SD",
                "SF",
                "SW",
            ]:
                value = TRACE_VALUE
        # Handle variable time length data
        if varname[2] == "V":
            itime = line[87:91]
            if itime[0] == "2":
                varname = "%sDVD%s" % (varname, itime[-2:])
        # Handle 7.4.6 Paired Value ("Vector") Physical Elements
        if varname[:2] in [
            "HQ",
            "MD",
            "MN",
            "MS",
            "MV",
            "NO",
            "ST",
            "TB",
            "TE",
            "TV",
        ]:
            depth = int(value)
            if depth == 0:
                value = abs(value * 1000)
            else:
                value = abs((value * 1000) % (depth * 1000))
            if depth < 0:
                value = 0 - value
                depth = abs(depth)
            varname = "%s.%02i" % (varname, depth)
            if len(varname) > 10:
                if depth > 999:
                    LOG.info(
                        "Ignoring sid: %s varname: %s value: %s",
                        sid,
                        varname,
                        value,
                    )
                    continue
                common.email_error(
                    ("sid: %s varname: %s value: %s " "is too large")
                    % (sid, varname, value),
                    "%s\n%s" % (data, prod.text),
                )
            continue
        st_data[varname] = value
    # Now we process each station we found in the report! :)
    for sid in mydata:
        times = list(mydata[sid].keys())
        times.sort()
        for tstamp in times:
            process_site(prod, sid, tstamp, mydata[sid][tstamp])


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
        """
            INSERT into unknown(nwsli, product, network)
            values ('%s', '%s', '%s')
        """
        % (sid, product_id, network)
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


def var2dbcols(var):
    """ Convert a SHEF var into split values """
    if var.find(".") > -1:
        parts = var.split(".")
        var = parts[0]
        return [var[:2], var[2], var[3:5], var[5], var[-1], parts[1]]
    else:
        return [var[:2], var[2], var[3:5], var[5], var[-1], None]


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
        (sid, varname) = k.split("|")
        (pe, d, s, e, p, depth) = var2dbcols(varname)
        d2 = ACCESSDB.runOperation(
            """
        INSERT into current_shef(station, valid, physical_code, duration,
        source, extremum, probability, value, depth)
        values (%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """,
            (
                sid,
                mydict["valid"].strftime("%Y-%m-%d %H:%M+00"),
                pe,
                d,
                s,
                e,
                p,
                mydict["value"],
                depth,
            ),
        )
        d2.addErrback(common.email_error, "")
        mydict["dirty"] = False

    LOG.info("save_current() processed %s entries, %s skipped", cnt, skipped)


def get_localtime(sid, ts):
    """Compute the local timestamp for this location"""
    if sid not in LOCS:
        return ts
    _network = list(LOCS[sid].keys())[0]
    return ts.astimezone(
        TIMEZONES.get(LOCS[sid][_network]["tzname"], pytz.utc)
    )


def get_network(prod, sid, _ts, data):
    """Figure out which network this belongs to"""
    networks = list(LOCS.get(sid, dict()).keys())
    # This is the best we can hope for
    if len(networks) == 1:
        return networks[0]
    # Our simple determination if the site is a COOP site
    is_coop = False
    if prod.afos[:3] == "RR3":
        is_coop = True
    elif prod.afos[:3] in ["RR1", "RR2"] and checkvars(list(data.keys())):
        LOG.info("Guessing COOP? %s %s %s", sid, prod.product_id, data.keys())
        is_coop = True
    pnetwork = "COOP" if is_coop else "DCP"
    # filter networks now
    networks = [s for s in networks if s.find(pnetwork) > 0]
    if len(networks) == 1:
        return networks[0]

    # If networks is zero length, then we have to try some things
    if not networks:
        enter_unknown(sid, prod.product_id, "")
        if len(sid) == 5:
            state = reference.nwsli2state.get(sid[3:])
            country = reference.nwsli2country.get(sid[3:])
            if country in ["CA", "MX"]:
                return "%s_%s_%s" % (country, state, pnetwork)
            elif country == "US":
                return "%s_%s" % (state, pnetwork)
            return "%s__%s" % (country, pnetwork)

    if sid not in UNKNOWN:
        UNKNOWN[sid] = True
        LOG.info(
            "get_network failure for sid: %s tp: %s", sid, prod.product_id
        )
    return None


def process_site(prod, sid, ts, data):
    """
    I process a dictionary of data for a particular site
    """
    localts = get_localtime(sid, ts)
    # Insert data into database regardless
    for varname in data:
        value = data[varname]
        deffer = HADSDB.runOperation(
            """INSERT into raw_inbound
                (station, valid, key, value)
                VALUES(%s,%s, %s, %s)
                """,
            (sid, ts.strftime("%Y-%m-%d %H:%M+00"), varname, value),
        )
        deffer.addErrback(common.email_error, prod.text)
        deffer.addErrback(LOG.error)

        key = "%s|%s" % (sid, varname)
        cur = CURRENT_QUEUE.setdefault(
            key, dict(valid=ts, value=value, dirty=True)
        )
        if ts > cur["valid"]:
            cur["valid"] = ts
            cur["value"] = value
            cur["dirty"] = True

    # Don't bother with stranger locations
    if len(sid) == 8 and sid[0] == "X":
        return
    # Don't bother with unknown sites
    if sid in UNKNOWN:
        return
    network = get_network(prod, sid, ts, data)
    if network in ["KCCI", "KIMT", "KELO", "ISUSM", None]:
        return

    # Do not send DCP sites with old data to IEMAccess
    if network.find("_DCP") > 0 and localts < LOCS.get(sid, dict()).get(
        "valid", localts
    ):
        return
    metadata = LOCS.setdefault(
        sid, {network: dict(valid=localts, tzname=None)}
    )
    metadata[network]["valid"] = localts

    # Okay, time for a hack, if our observation is at midnight!
    if localts.hour == 0 and localts.minute == 0:
        localts -= datetime.timedelta(minutes=1)
        # LOG.info("Shifting %s [%s] back one minute: %s" % (sid, network,
        #                                                  localts))

    iemob = Observation(sid, network, localts)
    iscoop = network.find("COOP") > 0
    hasdata = False
    for var in data:
        # shefit uses -9999 as a missing sentinel
        val = None if data[var] < -9998 else data[var]
        iemvar = MAPPING[var]
        if iemvar == "":
            continue
        if val is None:
            # Behold, glorious hack here to force nulls into the summary
            # database that uses coerce
            iemob.data["null_%s" % (iemvar,)] = None
        else:
            val *= MULTIPLIER.get(var[:2], 1.0)
        hasdata = True
        iemob.data[iemvar] = val
        if MAPPING[var] in ["pday", "snow", "snowd"]:
            # Convert 0.001 to 0.0001 for Trace values
            if val is not None and val == 0.001:
                iemob.data[iemvar] = TRACE_VALUE
            # Prevent negative numbers
            elif val is not None and val < 0:
                iemob.data[iemvar] = 0
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
    if hasdata:
        iemob.data["raw"] = prod.product_id

        deffer = ACCESSDB.runInteraction(iemob.save)
        deffer.addCallback(got_results, prod.product_id, sid, network, localts)
        deffer.addErrback(common.email_error, prod.text)
        deffer.addErrback(LOG.error)
    # else:
    #    print 'NODATA?', sid, network, localts, data


def got_results(res, product_id, sid, network, localts):
    """
    Callback after our iemdb work
    @param res response
    @param product_id product_id
    @param sid stationID
    @param network string network
    @param localts timestamptz of the observation
    """
    if res:
        return
    basets = datetime.datetime.utcnow().replace(tzinfo=pytz.utc)
    basets -= datetime.timedelta(hours=48)
    # If this is old data, we likely recently added this station and are
    # simply missing a database entry for it?
    if localts < basets:
        return
    enter_unknown(sid, product_id, network)


def service_guard():
    """Make sure things are not getting sideways

    When we call shutdown, this closes the inbound STDIN pipe, which termintes
    the LDM pqact process and starts up a new one.  This process then sits and
    spins as it works off its database queue.  This all is not ideal!
    """
    # all_objects = muppy.get_objects()
    # sum1 = summary.summarize(all_objects)
    # summary.print_(sum1)
    # LOG.info("DIFF--------------")
    # TR.print_diff()
    LOG.info(
        "service_guard jobs[waiting: %s, pending: %s] "
        "dbpool queuesz[hads:%s, access:%s]",
        len(JOBS.waiting),
        len(JOBS.pending),
        # pylint: disable=protected-access
        HADSDB.threadpool._queue.qsize(),
        ACCESSDB.threadpool._queue.qsize(),
    )
    if len(JOBS.pending) > 1000:
        LOG.info("Starting shutdown due to more than 1000 jobs in queue")
        common.shutdown()


def main2(_res):
    """
    Go main Go!
    """
    LOG.info("main() fired!")
    bridge(process_data)

    for _ in range(6):
        cooperate(worker())

    lc = LoopingCall(save_current)
    lc.start(373, now=False)
    lc2 = LoopingCall(service_guard)
    lc2.start(61, now=False)


def fullstop(err):
    """more forcable stop."""
    LOG.info("fullstop() called...")
    LOG.error(err)
    reactor.stop()


def main():
    """We startup."""
    # Need to find the shef_workspace folder
    origcwd = os.getcwd()
    path = None
    for path in get_search_paths():
        if os.path.isdir(os.path.join(path, "shef_workspace")):
            break
    path = os.path.join(path, "shef_workspace")
    LOG.info("Changing cwd to %s", path)
    os.chdir(path)

    # Load the station metadata before we fire up the ingesting
    df = HADSDB.runInteraction(load_stations)
    df.addCallback(main2)
    df.addErrback(fullstop)
    reactor.run()
    # For testing purposes
    os.chdir(origcwd)


if __name__ == "__main__":
    main()
