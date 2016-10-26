"""
 SHEF product ingestor
"""
# System Imports
import os
import datetime

# Twisted Python imports
from syslog import LOG_LOCAL2
from twisted.python import syslog
from twisted.python import log

# Stuff I wrote
from pyiem.observation import Observation
from pyiem.nws import product
from pyiem import reference
from pyldm import ldmbridge
import common  # @UnresolvedImport
import pytz

# Third Party Stuff
from twisted.internet import task
from twisted.internet.defer import DeferredQueue, Deferred
from twisted.internet.task import cooperate
from twisted.internet import reactor, protocol
from twisted.internet.task import LoopingCall

# Start Logging
syslog.startLogging(prefix='pyWWA/shef_parser', facility=LOG_LOCAL2)

# Setup Database Links
# the current_shef table is not very safe when two processes attempt to update
# it at the same time, use a single process for this connection
ACCESSDB = common.get_database('iem', module_name='psycopg2', cp_max=10)
HADSDB = common.get_database('hads', module_name='psycopg2', cp_max=10)


# stations we don't know about
UNKNOWN = dict()
# station metadata
LOCS = dict()
# database timezones to pytz cache
TIMEZONES = dict()
# a queue for saving database IO
CURRENT_QUEUE = {}
U1980 = datetime.datetime.utcnow()
U1980 = U1980.replace(day=1, year=1980, tzinfo=pytz.timezone("UTC"))


def load_stations(txn):
    """Load station metadata

    We need this information as we can't reliably convert a station ID found
    in a SHEF encoded product to a network that is necessary to update IEM
    Access.

    Args:
      txn: a database transaction
    """
    log.msg("load_stations called...")
    txn.execute("""
        SELECT id, network, tzname from stations
        WHERE network ~* 'COOP' or network ~* 'DCP' or
        network in ('KCCI','KIMT','KELO', 'ISUSM')
        ORDER by network ASC
        """)

    LOCS.clear()  # clear out our current cache
    for (stid, network, tzname) in txn:
        if stid in UNKNOWN:
            log.msg("  station: %s is no longer unknown!" % (stid,))
            UNKNOWN.pop(stid)
        if tzname is None or tzname == '':
            log.msg("  station: %s has tzname: %s" % (stid, tzname))
        metadata = LOCS.setdefault(stid, dict())
        if network not in metadata:
            metadata[network] = dict(valid=U1980, tzname=tzname)
        if tzname not in TIMEZONES:
            try:
                TIMEZONES[tzname] = pytz.timezone(tzname)
            except:
                log.msg("pytz does not like tzname: %s" % (tzname,))
                TIMEZONES[tzname] = pytz.utc

    log.msg("loaded %s stations" % (len(LOCS),))
    # Reload every 12 hours
    reactor.callLater(12*60*60, HADSDB.runInteraction, load_stations)


MULTIPLIER = {"US": 0.87,  # Convert MPH to KNT
              "UG": 0.87,
              "UP": 0.87,
              "UR": 10,
              }

"""
Some notes on the SHEF codes translated to something IEM Access can handle
First two chars are physical extent code

"""
DIRECTMAP = {
    'HGIZ': 'rstage',
    'HPIZ': 'rstage',
    'HTIZ': 'rstage',

    'PAIZ': 'pres',
    'PPHZ': 'phour',
    'PPDZ': 'pday',
    'PPPZ': 'pday',
    'PCIZ': 'pcounter',

    'QRIZ': 'discharge',
    'QTIZ': 'discharge',

    'RWIZ': 'srad',

    'SDIZ': 'snowd',
    'SFDZ': 'snow',
    'SWIZ': 'snoww',

    'TDIZ': 'dwpf',
    'TAIZ': 'tmpf',
    'TAIN': 'min_tmpf',
    'TAIX': 'max_tmpf',
    'TWIZ': 'water_tmpf',

    'UDIZ': 'drct',
    'UGIZ': 'gust',
    'UPIZ': 'gust',
    'UPHZ': 'gust',
    'URHZ': 'max_drct',
    'URIZ': 'max_drct',
    'USIZ': 'sknt',  # note above that we apply a multipler

    'VBIZ': 'battery',

    'XRIZ': 'relh',

    'XVIZ': 'vsby',
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
        else:
            log.msg('Can not map var %s' % (key,))
            self.__setitem__(key, '')
            return ''

MAPPING = MyDict()


class SHEFIT(protocol.ProcessProtocol):
    """
    My process protocol for dealing with the SHEFIT program from the NWS
    """

    def __init__(self, buf):
        """
        Constructor
        """
        self.tp = product.TextProduct(buf)
        self.data = ""

    def connectionMade(self):
        """
        Fired when the program starts up and wants stdin
        """
        # print "sending %d bytes!" % len(self.shefdata)
        # print "SENDING", self.shefdata
        self.transport.write(self.tp.text)
        self.transport.closeStdin()

    def outReceived(self, data):
        """
        Save the stdout we get from the program for later processing
        """
        # print "GOT", data
        self.data = self.data + data

    def errReceived(self, data):
        """
        In case something comes to stderr
        """
        log.msg("errReceived! with %d bytes!" % len(data))
        log.msg(data)
        self.deferred.errback(data)

#    def processEnded(self, status):
#        print "debug: type(status): %s" % type(status.value)
#        print "error: exitCode: %s" % status.value.exitCode

    def outConnectionLost(self):
        """
        Once the program is done, we need to do something with the data
        """
        # if self.data == "":
        #    rejects = open("empty.shef", 'a')
        #    rejects.write( self.tp.raw +"\003")
        #    rejects.close()
        #    return
        t = task.deferLater(reactor, 0, really_process, self.tp, self.data)
        t.addErrback(common.email_error, self.tp.text)
        self.deferred.callback(self)


def clnstr(buf):
    """
    Get rid of cruft we don't wish to work with
    """
    return buf.replace("\015\015\012",
                       "\n").replace("\003", "").replace("\001", "")


class MyProductIngestor(ldmbridge.LDMProductReceiver):

    def connectionLost(self, reason):
        log.msg('connectionLost')
        log.err(reason)
        reactor.callLater(15, self.shutdown)

    def shutdown(self):
        reactor.callWhenRunning(reactor.stop)

    def process_data(self, buf):
        """
        I am called from the ldmbridge when data is ahoy
        """
        self.jobs.put(clnstr(buf))


def async(buf):
    """
    Async caller of reactor processes
    @param buf string of the raw NOAAPort Product
    """
    defer = Deferred()
    proc = SHEFIT(buf)
    proc.deferred = defer
    proc.deferred.addErrback(log.err)

    reactor.spawnProcess(proc, "shefit", ["shefit"], {})
    return proc.deferred


def worker(jobs):
    while True:
        yield jobs.get().addCallback(async).addErrback(common.email_error,
                                                       'Unhandled Error'
                                                       ).addErrback(log.err)


def make_datetime(dpart, tpart):
    """Create a datatime instance from these two strings"""
    if dpart == "0000-00-00":
        return None
    dstr = "%s %s" % (dpart, tpart)
    tstamp = datetime.datetime.strptime(dstr, "%Y-%m-%d %H:%M:%S")
    return tstamp.replace(tzinfo=pytz.timezone("UTC"))


def really_process(tp, data):
    """
    This processes the output we get from the SHEFIT program
    """
    # Now we loop over the data we got :)
    # log.msg("\n"+data)
    mydata = {}
    for line in data.split("\n"):
        # Skip blank output lines
        if line.strip() == "":
            continue
        tokens = line.split()
        if len(tokens) < 7:
            log.msg("NO ENOUGH TOKENS %s" % (line,))
            continue
        sid = tokens[0]
        if len(sid) > 8:
            log.msg("SiteID Len Error: [%s] %s" % (sid, tp.get_product_id()))
            continue
        if sid not in mydata:
            mydata[sid] = {}
        tstamp = make_datetime(tokens[1], tokens[2])
        modelruntime = make_datetime(tokens[3], tokens[4])
        if modelruntime is not None:
            # print("Skipping forecast data for %s" % (sid, ))
            continue
        # We don't care about data in the future!
        utcnow = datetime.datetime.utcnow().replace(tzinfo=pytz.timezone("UTC"
                                                                         ))
        if tstamp > (utcnow + datetime.timedelta(hours=1)):
            continue
        if tstamp < (utcnow - datetime.timedelta(days=60)):
            log.msg("Rejecting old data %s %s" % (sid, tstamp))
            continue
        s_data = mydata.setdefault(sid, dict())
        st_data = s_data.setdefault(tstamp, dict())

        varname = tokens[5]
        if tokens[6].find("****") == 0:
            log.msg("Bad Data from %s\n%s" % (tp.get_product_id(), data))
            value = -9999.0
        else:
            value = float(tokens[6])
        # Handle 7.4.6 Paired Value ("Vector") Physical Elements
        if varname[:2] in ['HQ', 'MD', 'MN', 'MS', 'MV', 'NO', 'ST', 'TB',
                           'TE', 'TV']:
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
                    log.msg(("Ignoring sid: %s varname: %s value: %s"
                             ) % (sid, varname, value))
                    continue
                common.email_error(("sid: %s varname: %s value: %s "
                                    "is too large") % (sid, varname, value),
                                   "%s\n%s" % (data, tp.unixtext))
            continue
        st_data[varname] = value
    # Now we process each station we found in the report! :)
    for sid in mydata.keys():
        times = mydata[sid].keys()
        times.sort()
        for tstamp in times:
            process_site(tp, sid, tstamp, mydata[sid][tstamp])


def enter_unknown(sid, product_id, network):
    """
    Enter some info about a site ID we know nothing of...
    @param sid string site id
    @param product_id string
    @param network string of the guessed network
    """
    # Eh, lets not care about non-5 char IDs
    if len(sid) != 5:
        return
    # log.msg("Found unknown %s %s %s" % (sid, tp.get_product_id(), network))
    HADSDB.runOperation("""
            INSERT into unknown(nwsli, product, network)
            values ('%s', '%s', '%s')
        """ % (sid, product_id, network))


def checkvars(myvars):
    """
    Check variables to see if we have a COOP or DCP site
    """
    for v in myvars:
        # Definitely DCP
        if v[:2] in ['HG', ]:
            return False
        if v[:2] in ['SF', 'SD']:
            return True
        if v[:3] in ['PPH', ]:
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
        if not mydict['dirty']:
            skipped += 1
            continue
        cnt += 1
        (sid, varname) = k.split("|")
        (pe, d, s, e, p, depth) = var2dbcols(varname)
        d2 = ACCESSDB.runOperation("""
        INSERT into current_shef(station, valid, physical_code, duration,
        source, extremum, probability, value, depth)
        values (%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """, (sid, mydict['valid'].strftime("%Y-%m-%d %H:%M+00"), pe, d, s, e,
              p, mydict['value'], depth))
        d2.addErrback(common.email_error, '')
        mydict['dirty'] = False

    log.msg("save_current() processed %s entries, %s skipped" % (cnt, skipped))


def get_localtime(sid, ts):
    """Compute the local timestamp for this location"""
    if sid not in LOCS:
        return ts
    _network = LOCS[sid].keys()[0]
    return ts.astimezone(TIMEZONES.get(LOCS[sid][_network]['tzname'],
                                       pytz.utc))


def get_network(tp, sid, ts, data):
    """Figure out which network this belongs to"""
    networks = LOCS.get(sid, dict()).keys()
    # This is the best we can hope for
    if len(networks) == 1:
        return networks[0]
    # Our simple determination if the site is a COOP site
    is_coop = False
    if tp.afos[:3] == 'RR3':
        is_coop = True
    elif tp.afos[:3] in ['RR1', 'RR2'] and checkvars(data.keys()):
        log.msg("Guessing COOP? %s %s %s" % (sid, tp.get_product_id(),
                                             data.keys()))
        is_coop = True
    pnetwork = 'COOP' if is_coop else 'DCP'
    # filter networks now
    networks = [s for s in networks if s.find(pnetwork) > 0]
    if len(networks) == 1:
        return networks[0]

    # If networks is zero length, then we have to try some things
    if len(networks) == 0:
        enter_unknown(sid, tp.get_product_id(), "")
        if len(sid) == 5:
            state = reference.nwsli2state.get(sid[3:])
            country = reference.nwsli2country.get(sid[3:])
            if country in ['CA', 'MX']:
                return "%s_%s_%s" % (country, state, pnetwork)
            elif country == 'US':
                return "%s_%s" % (state, pnetwork)
            else:
                return "%s__%s" % (country, pnetwork)

    if sid not in UNKNOWN:
        UNKNOWN[sid] = True
        log.msg(("get_network failure for sid: %s tp: %s"
                 ) % (sid, tp.get_product_id()))
    return None


def process_site(tp, sid, ts, data):
    """
    I process a dictionary of data for a particular site
    """
    localts = get_localtime(sid, ts)
    # log.msg("%s sid: %s ts: %s %s" % (tp.get_product_id(), sid, ts, localts))
    # Insert data into database regardless
    for varname in data.keys():
        value = data[varname]
        deffer = HADSDB.runOperation("""INSERT into raw_inbound
                (station, valid, key, value)
                VALUES(%s,%s, %s, %s)
                """, (sid, ts.strftime("%Y-%m-%d %H:%M+00"), varname, value))
        deffer.addErrback(common.email_error, tp.text)
        deffer.addErrback(log.err)

        key = "%s|%s" % (sid, varname)
        cur = CURRENT_QUEUE.setdefault(key, dict(valid=ts, value=value,
                                                 dirty=True))
        if ts > cur['valid']:
            cur['valid'] = ts
            cur['value'] = value
            cur['dirty'] = True

    # Don't bother with stranger locations
    if len(sid) == 8 and sid[0] == 'X':
        return
    # Don't bother with unknown sites
    if sid in UNKNOWN:
        return
    network = get_network(tp, sid, ts, data)
    if network in ['KCCI', 'KIMT', 'KELO', 'ISUSM', None]:
        return

    # Do not send DCP sites with old data to IEMAccess
    if network.find("_DCP") > 0 and localts < LOCS.get(
            sid, dict()).get('valid', localts):
        return
    metadata = LOCS.setdefault(sid,
                               {network: dict(valid=localts, tzname=None)})
    metadata[network]['valid'] = localts

    # Okay, time for a hack, if our observation is at midnight!
    if localts.hour == 0 and localts.minute == 0:
        localts -= datetime.timedelta(minutes=1)
        # log.msg("Shifting %s [%s] back one minute: %s" % (sid, network,
        #                                                  localts))

    iemob = Observation(sid, network, localts)
    iscoop = (network.find('COOP') > 0)
    hasdata = False
    for var in data.keys():
        if data[var] == -9999:
            continue
        iemvar = MAPPING[var]
        if iemvar == '':
            continue
        hasdata = True
        myval = data[var] * MULTIPLIER.get(var[:2], 1.0)
        iemob.data[MAPPING[var]] = myval
        # Convert 0.001 to 0.0001 for Trace values
        if myval == 0.001 and MAPPING[var] in ['pday', 'snow', 'snowd']:
            iemob.data[MAPPING[var]] = 0.0001
        if iscoop:
            # Save COOP 'at-ob' temperature into summary table
            if MAPPING[var] == 'tmpf':
                iemob.data['coop_tmpf'] = myval
            # Save observation time into the summary table
            if MAPPING[var] in ['tmpf', 'max_tmpf', 'min_tmpf', 'pday',
                                'snow', 'snowd']:
                iemob.data['coop_valid'] = iemob.data['valid']
    if hasdata:
        iemob.data['raw'] = tp.get_product_id()

        deffer = ACCESSDB.runInteraction(iemob.save)
        deffer.addCallback(got_results, tp.get_product_id(), sid, network)
        deffer.addErrback(common.email_error, tp.text)
        deffer.addErrback(log.err)
    # else:
    #    print 'NODATA?', sid, network, localts, data


def got_results(res, product_id, sid, network):
    """
    Callback after our iemdb work
    @param res response
    @param product_id product_id
    @param sid stationID
    @param network string network
    """
    if not res:
        enter_unknown(sid, product_id, network)


def job_size(jobs):
    """
    Print out some debug information to the log on the current size of the
    job queue
    """
    log.msg("deferredQueue waiting: %s pending: %s" % (len(jobs.waiting),
                                                       len(jobs.pending)))
    if len(jobs.pending) > 1000:
        log.msg("ABORT")
        reactor.callWhenRunning(reactor.stop)
    reactor.callLater(300, job_size, jobs)


def main(res):
    """
    Go main Go!
    """
    log.msg("main() fired!")
    jobs = DeferredQueue()
    ingest = MyProductIngestor()
    ingest.jobs = jobs
    ldmbridge.LDMProductFactory(ingest)

    for _ in range(3):
        cooperate(worker(jobs))

    reactor.callLater(300, job_size, jobs)
    lc = LoopingCall(save_current)
    lc.start(373, now=False)


def fullstop(err):
    log.msg("fullstop() called...")
    log.err(err)
    reactor.stop()


def bootstrap():
    # Necessary for the shefit program to run A-OK
    mydir = os.path.dirname(os.path.abspath(__file__))
    path = os.path.normpath(os.path.join(mydir, "..", "shef_workspace"))
    log.msg("Changing cwd to %s" % (path,))
    os.chdir(path)

    # Load the station metadata before we fire up the ingesting
    df = HADSDB.runInteraction(load_stations)
    df.addCallback(main)
    df.addErrback(fullstop)
    reactor.run()

if __name__ == '__main__':
    bootstrap()
