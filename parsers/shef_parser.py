"""
 SHEF product ingestor
"""
# System Imports
import os
import datetime

# Twisted Python imports
from syslog import LOG_LOCAL2
from twisted.python import syslog
syslog.startLogging(prefix='pyWWA/shef_parser', facility=LOG_LOCAL2)
from twisted.python import log

# Stuff I wrote
from pyiem.observation import Observation
from pyiem.nws import product
from pyiem import reference
from pyldm import ldmbridge
import common
import pytz

# Third Party Stuff
from twisted.internet import task
from twisted.internet.defer import DeferredQueue, Deferred
from twisted.internet.task import cooperate
from twisted.internet import reactor, protocol

# Setup Database Links
# the current_shef table is not very safe when two processes attempt to update
# it at the same time, use a single process for this connection
ACCESSDB_SINGLE = common.get_database('iem', cp_max=1)
ACCESSDB = common.get_database('iem')
HADSDB = common.get_database('hads')

# Necessary for the shefit program to run A-OK
_MYDIR = os.path.dirname(os.path.abspath(__file__))
PATH = os.path.normpath(os.path.join(_MYDIR,
                                     "..", "shef_workspace"))
log.msg("Changing cwd to %s" % (PATH,))
os.chdir(PATH)

# Load up our lookup table of stations to networks
LOC2STATE = {}
LOC2NETWORK = {}
LOC2TZ = {}
LOC2VALID = {}
TIMEZONES = {None: pytz.timezone('UTC')}


def load_stations(txn):
    """
    Load up station metadata to help us with writing data to the IEM database
    @param txn database transaction
    """
    log.msg("load_stations called...")
    txn.execute("""SELECT id, network, state, tzname from stations
        WHERE network ~* 'COOP' or network ~* 'DCP' or
        network in ('KCCI','KIMT','KELO', 'ISUSM')
        ORDER by network ASC""")
    u1980 = datetime.datetime.utcnow()
    u1980 = u1980.replace(day=1, year=1980, tzinfo=pytz.timezone("UTC"))
    for row in txn:
        stid = row['id']
        LOC2VALID.setdefault(stid, u1980)
        LOC2STATE[stid] = row['state']
        LOC2TZ[stid] = row['tzname']
        if stid in LOC2NETWORK:
            del LOC2NETWORK[stid]
        else:
            LOC2NETWORK[stid] = row['network']
        if row['tzname'] not in TIMEZONES:
            try:
                TIMEZONES[row['tzname']] = pytz.timezone(row['tzname'])
            except:
                log.msg("pytz does not like tzname: %s" % (row['tzname'],))
                TIMEZONES[row['tzname']] = pytz.timezone("UTC")

    log.msg("loaded %s stations" % (len(LOC2STATE),))


MULTIPLIER = {"US": 0.87,  # Convert MPH to KNT
              "UG": 0.87,
              "UP": 0.87,
              "UR": 10,
              }

"""
Some notes on the SHEF codes translated to something IEM Access can handle
First two chars are physical extent code

"""
DIRECTMAP = {'URIZ': 'max_drct',
             'XVIZ': 'vsby',
             'HGIZ': 'rstage',
             'HPIZ': 'rstage',
             'HTIZ': 'rstage',
             'PPHZ': 'phour',
             'TDIZ': 'dwpf',
             'TAIZ': 'tmpf',
             'TAIN': 'min_tmpf',
             'TAIX': 'max_tmpf',
             'PPDZ': 'pday',
             'PPPZ': 'pday',
             'PCIZ': 'pcounter',
             'RWIZ': 'srad',
             'SDIZ': 'snowd',
             'XRIZ': 'relh',
             'PAIZ': 'pres',
             'QRIZ': 'discharge',
             'QTIZ': 'discharge',
             'SWIZ': 'snoww',
             'USIZ': 'sknt',
             'SFDZ': 'snow',
             'UDIZ': 'drct',
             'UGIZ': 'gust',
             'UPIZ': 'gust',
             'UPHZ': 'gust',
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
        dstr = "%s %s" % (tokens[1], tokens[2])
        tstamp = datetime.datetime.strptime(dstr, "%Y-%m-%d %H:%M:%S")
        tstamp = tstamp.replace(tzinfo=pytz.timezone("UTC"))
        # We don't care about data in the future!
        utcnow = datetime.datetime.utcnow().replace(tzinfo=pytz.timezone("UTC"
                                                                         ))
        if tstamp > (utcnow + datetime.timedelta(hours=1)):
            continue
        if tstamp < (utcnow - datetime.timedelta(days=60)):
            log.msg("Rejecting old data %s %s" % (sid, tstamp))
            continue
        if tstamp not in mydata[sid]:
            mydata[sid][tstamp] = {}

        varname = tokens[5]
        if tokens[6].find("****") == 0:
            log.msg("Bad Data from %s\n%s" % (tp.get_product_id(), data))
            value = -9999.0
        else:
            value = float(tokens[6])
        if varname[:2] in ['TV', 'TB']:  # Soil Depth fun!
            depth = int(value)
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
        mydata[sid][tstamp][varname] = value
    # Now we process each station we found in the report! :)
    for sid in mydata.keys():
        times = mydata[sid].keys()
        times.sort()
        for tstamp in times:
            process_site(tp, sid, tstamp, mydata[sid][tstamp])


def enter_unknown(sid, tp, network):
    """
    Enter some info about a site ID we know nothing of...
    @param sid string site id
    @param tp TextProduct instance
    @param network string of the guessed network
    """
    if len(sid) < 5:
        return
    # log.msg("Found unknown %s %s %s" % (sid, tp.get_product_id(), network))
    HADSDB.runOperation("""
            INSERT into unknown(nwsli, product, network)
            values ('%s', '%s', '%s')
        """ % (sid, tp.get_product_id(), network))


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


def process_site(tp, sid, ts, data):
    """
    I process a dictionary of data for a particular site
    """
    localts = ts.astimezone(TIMEZONES[LOC2TZ.get(sid)])
    # log.msg("%s sid: %s ts: %s %s" % (tp.get_product_id(), sid, ts, localts))
    # Insert data into database regardless
    for varname in data.keys():
        value = data[varname]
        deffer = HADSDB.runOperation("""INSERT into raw""" +
                                     ts.strftime("%Y_%m") +
                                     """ (station, valid, key, value)
                                     VALUES(%s,%s, %s, %s)""",
                                     (sid, ts.strftime("%Y-%m-%d %H:%M+00"),
                                      varname, value))
        deffer.addErrback(common.email_error, tp.text)
        deffer.addErrback(log.err)

        (pe, d, s, e, p, depth) = var2dbcols(varname)
        d2 = ACCESSDB_SINGLE.runOperation("""
        INSERT into current_shef(station, valid, physical_code, duration,
        source, extremum, probability, value, depth)
        values (%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """, (sid, ts.strftime("%Y-%m-%d %H:%M+00"), pe, d, s, e, p, value,
              depth))
        d2.addErrback(common.email_error, tp.text)

    # Our simple determination if the site is a COOP site
    is_coop = False
    if tp.afos[:3] == 'RR3':
        is_coop = True
    elif tp.afos[:3] in ['RR1', 'RR2'] and checkvars(data.keys()):
        log.msg("Guessing COOP? %s %s %s" % (sid, tp.get_product_id(),
                                             data.keys()))
        is_coop = True

    state = LOC2STATE.get(sid)
    if state is None and len(sid) == 8 and sid[0] == 'X':
        return
    # Base the state on the 2 char station ID!
    if state is None and len(sid) == 5 and sid[3:] in reference.nwsli2state:
        state = reference.nwsli2state.get(sid[3:])
        LOC2STATE[sid] = state
    if state is None:
        enter_unknown(sid, tp, "")
        return

    # Deterime if we want to waste the DB's time
    network = LOC2NETWORK.get(sid)
    if network in ['KCCI', 'KIMT', 'KELO', 'ISUSM']:
        return
    if network is None:
        if is_coop:
            network = "%s_COOP" % (state,)
        # We are left with DCP
        else:
            country = reference.nwsli2country.get(sid[3:])
            if country in ['CA', 'MX']:
                network = "%s_%s_DCP" % (country, state)
            elif country == 'US':
                network = "%s_DCP" % (state,)
            else:
                network = "%s__DCP" % (country,)

    # Do not send DCP sites with old data to IEMAccess
    if network.find("_DCP") > 0 and localts < LOC2VALID.get(sid, localts):
        return
    LOC2VALID[sid] = localts

    # Okay, time for a hack, if our observation is at midnight!
    if localts.hour == 0 and localts.minute == 0:
        localts -= datetime.timedelta(minutes=1)
        # log.msg("Shifting %s [%s] back one minute: %s" % (sid, network,
        #                                                  localts))

    iemob = Observation(sid, network, localts)

    deffer = ACCESSDB.runInteraction(save_data, tp, iemob, data)
    deffer.addCallback(got_results, tp, sid, network)
    deffer.addErrback(common.email_error, tp.text)
    deffer.addErrback(log.err)


def got_results(res, tp, sid, network):
    """
    Callback after our iemdb work
    @param res response
    @param tp pyiem.nws.product.TextProduct instance
    @param sid stationID
    @param network string network
    """
    if not res:
        enter_unknown(sid, tp, network)


def save_data(txn, tp, iemob, data):
    """
    Called from a transaction 'thread'
    """
    iscoop = (iemob.data['network'].find('COOP') > 0)
    for var in data.keys():
        if data[var] == -9999:
            continue
        myval = data[var] * MULTIPLIER.get(var[:2], 1.0)
        iemob.data[MAPPING[var]] = myval
        if iscoop:
            # Save COOP 'at-ob' temperature into summary table
            if MAPPING[var] == 'tmpf':
                iemob.data['coop_tmpf'] = myval
            # Save observation time into the summary table
            if MAPPING[var] in ['tmpf', 'max_tmpf', 'min_tmpf', 'pday',
                                'snow', 'snowd']:
                iemob.data['coop_valid'] = iemob.data['valid']
    iemob.data['raw'] = tp.get_product_id()
    return iemob.save(txn)


def dump_memory():
    """Dump some memory stats"""
    from pympler import muppy
    from pympler import summary
    all_objects = muppy.get_objects()
    sum1 = summary.summarize(all_objects)
    summary.print_(sum1)
    reactor.callLater(300, dump_memory)


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
    reactor.callLater(60, dump_memory)


def fullstop(err):
    log.msg("fullstop() called...")
    log.err(err)
    reactor.stop()

if __name__ == '__main__':
    df = HADSDB.runInteraction(load_stations)
    df.addCallback(main)
    df.addErrback(fullstop)
    reactor.run()
