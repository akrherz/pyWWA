""" 
  Take the NCR NEXRAD Level III product and run it through gpnids to get the
  attribute table, which we then dump into the database 
"""
# System Imports
import os
import math

# Setup Standard Logging we use
# Twisted Python imports
from syslog import LOG_LOCAL2
from twisted.python import syslog
syslog.startLogging(prefix='pyWWA/nexrad3_attr', facility=LOG_LOCAL2)
from twisted.python import log


# Need to do this in order to get the subsequent calls to work??
os.chdir("/home/ldm/pyWWA/parsers")

# Stuff I wrote
from pyldm import ldmbridge

# Third Party Stuff
from twisted.internet.defer import DeferredQueue, Deferred
from twisted.internet.task import cooperate
from twisted.internet import reactor, protocol
import datetime
import pytz
import common

# Setup Database Links
POSTGISDB = common.get_database('postgis')

ST = {}

# For archive reprocessing, we need to specify the month and year
_UTCNOW = None
if os.environ.has_key("YYYY"):
    _UTCNOW = datetime.datetime(int(os.environ['YYYY']), 
                                int(os.environ['MM']), 1)
    _UTCNOW = _UTCNOW.replace(tzinfo=pytz.timezone("UTC"))
    log.msg("Date is hard coded to %s" % (_UTCNOW,))

def load_station_table(txn):
    """ Load the station table of NEXRAD sites """
    log.msg("load_station_table called() ...")
    txn.execute("""
        SELECT id, ST_x(geom) as lon, ST_y(geom) as lat from stations 
        where network in ('NEXRAD','TWDR')
    """)
    for row in txn:
        ST[row['id']] = {'lat': row['lat'],
                         'lon': row['lon']}
    log.msg("Station Table size %s" % (len(ST.keys(),)))

def write_pid():
    """ Create a PID file for when we are fired up! """
    pid = open("nexrad3_attr.pid",'w')
    pid.write("%s" % ( os.getpid(),) )
    pid.close()


def compute_ts(tstring):
    """ Figure out the timestamp of this product """
    #log.msg("tstring is %s" % (tstring,))
    day = int(tstring[12:14])
    hour = int(tstring[14:16])
    minute = int(tstring[16:18])

    if _UTCNOW is None:
        utc = datetime.datetime.utcnow()
        utc = utc.replace(tzinfo=pytz.timezone("UTC"), second=0, microsecond=0,
                          hour=hour, minute=minute)
        if utc.day > 25 and day == 1: # Next month!
            utc += datetime.timedelta(days=15) # careful
        if utc.day == 1 and day > 25: # Last month!
            utc -= datetime.timedelta(days=15)
        utc = utc.replace(day=day)    
    else:
        utc = _UTCNOW.replace(day=day, hour=hour, minute=minute)

    return utc

class PROC(protocol.ProcessProtocol):
    """
    My process protocol 
    """

    def __init__(self, buf):
        """
        Constructor
        """
        #log.msg("init() of PROC")
        self.res = ""
        self.ts = None
        self.wmo = None
        self.afos = None
        self.deferred = None
        self.buf = buf

        lines = buf.split("\r\r\n")
        if len(lines) < 4:
            log.msg("INCOMPLETE PRODUCT!")
            return
        self.wmo = lines[2]
        self.afos = lines[3]
        self.ts = compute_ts( self.wmo )
        #log.msg("end of init() of PROC")
        

    def connectionMade(self):
        """
        Fired when the program starts up and wants stdin
        """
        #log.msg("Connection Made")
        self.transport.write( self.buf +"\r\r\n\003")
        self.transport.closeStdin()

    def outReceived(self, data):
        """
        Save the stdout we get from the program for later processing
        """
        #log.msg("Got %d bytes!" % len(data))
        self.res += data

    def errReceived(self, data):
        """
        In case something comes to stderr 
        """
        log.msg("errReceived! with %d bytes!" % len(data))
        log.msg( data )
        if not isinstance(data, str):
            self.deferred.errback(data)


    def cancelDB(self, _):
        """ cancel DB session"""
        #log.msg("cancelDB()")
        self.deferred.callback(self)

    def log_error(self, err):
        """ Log an error """
        log.msg( self.res )
        log.msg( err )
        common.email_error(err, self.res)

    def outConnectionLost(self):
        """
        Once the program is done, we need to do something with the data
        """
        #log.msg("Teardown")
        if self.res == '' or self.res.find("NO STORMS DETECTED") > -1:
            defer = POSTGISDB.runInteraction(delete_prev_attrs, self.afos[3:])
            defer.addErrback( log.err )
            self.deferred.callback(self)
            return
        defer = POSTGISDB.runInteraction(really_process, self.res, 
                                         self.afos[3:], self.ts)
        defer.addCallback(self.cancelDB)
        defer.addErrback( self.log_error )
        defer.addErrback( log.err )
        

class MyProductIngestor(ldmbridge.LDMProductReceiver):
    """ My ingest protocol """
    
    def connectionLost(self, reason):
        """ Called when stdin is closed """
        log.msg('connectionLost')
        log.err(reason)
        reactor.callLater(15, reactor.callWhenRunning, reactor.stop)

    def process_data(self, buf):
        """
        I am called from the ldmbridge when data is ahoy
        """
        self.jobs.put( buf )

def delete_prev_attrs(txn, nexrad ):
    ''' Remove any previous attributes for this nexrad '''
    txn.execute("DELETE from nexrad_attributes WHERE nexrad = %s", (nexrad,))

def async(buf):
    """
    Async caller of reactor processes
    @param buf string of the raw NOAAPort Product
    """
    #log.msg('async() called...')
    defer = Deferred()
    proc = PROC(buf)
    proc.deferred = defer
    proc.deferred.addErrback( log.err )
    if proc.afos is not None:
        log.msg("PROCESS %s %s" % (proc.afos, proc.ts.strftime("%Y%m%d%H%M") ))
    
        reactor.spawnProcess(proc, "python", 
                   ["python", "ncr2postgis.py", proc.afos[3:],
                    proc.ts.strftime("%Y%m%d%H%M")], {})
    else:
        proc.cancelDB('bogus')
    return proc.deferred

def worker(jobs):
    """ I am a worker that processes jobs """
    while True:
        yield jobs.get().addCallback(async).addErrback( 
                common.email_error, 'Unhandled Error' ).addErrback( log.err )

def really_process(txn, res, nexrad, ts):
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
    delete_prev_attrs(txn, nexrad)
    
    cenlat = float(ST[nexrad]['lat'])
    cenlon = float(ST[nexrad]['lon'])
    latscale = 111137.0
    lonscale = 111137.0 * math.cos( cenlat * math.pi / 180.0 )

    #   STM ID  AZ/RAN TVS  MESO POSH/POH/MX SIZE VIL DBZM  HT  TOP  FCST MVMT
    lines = res.split("\n")
    co = 0
    for line in lines:
        if len(line) < 5:
            continue
        if line[1] != " ":
            continue
        tokens = line.replace(">", " ").replace("/", " ").split()
        if len(tokens) < 1 or tokens[0] == "STM":
            continue
        if tokens[5] == 'UNKNOWN':
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
        if tokens[2] == '***':
            log.msg("skipping bad line |%s|" % (line,))
            continue
        d["range"] = float(tokens[2]) * 1.852
        d["tvs"] = tokens[3]
        d["meso"] = tokens[4]
        d["posh"] = tokens[5]
        d["poh"] = tokens[6] if tokens[6] != "***" else None
        if (tokens[7] == "<0.50"):
            tokens[7] = 0.01
        d["max_size"] = tokens[7]

        if tokens[8] == 'UNKNOWN':
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
        d["nexrad"] = nexrad

        cosaz = math.cos( d["azimuth"] * math.pi / 180.0 )
        sinaz = math.sin( d["azimuth"] * math.pi / 180.0 )
        mylat = cenlat + (cosaz * (d["range"] * 1000.0) / latscale)
        mylon = cenlon + (sinaz * (d["range"] * 1000.0) / lonscale)
        d["geom"] = "SRID=4326;POINT(%s %s)" % (mylon, mylat)
        d["valid"] = ts

        for table in ['nexrad_attributes', 'nexrad_attributes_%s' % (ts.year,)]:
            sql = """INSERT into """+table+""" (nexrad, storm_id, geom, azimuth,
    range, tvs, meso, posh, poh, max_size, vil, max_dbz, max_dbz_height,
    top, drct, sknt, valid) values (%(nexrad)s, %(storm_id)s, %(geom)s,
    %(azimuth)s, %(range)s, %(tvs)s, %(meso)s, %(posh)s,
    %(poh)s, %(max_size)s, %(vil)s, %(max_dbz)s,
    %(max_dbz_height)s, %(top)s, %(drct)s, %(sknt)s, %(valid)s)"""
            txn.execute( sql, d )

    if co == 0:
        # Had a problem with GEMPAK corrupting its last.nts and/or gemglb.nts,
        # so when the ingestor senses trouble (no output), remove these files
        # and continue happily on
        log.msg("Got zero entries ||%s||" % (res,))
        for fn in ['gemglb.nts', 'last.nts']:
            if os.path.isfile(fn):
                os.unlink(fn)
    log.msg("%s %s Processed %s entries" % (nexrad, ts, co))

    
def job_size(jobs):
    """
    Print out some debug information to the log on the current size of the
    job queue
    """
    log.msg("deferredQueue waiting: %s pending: %s" % (len(jobs.waiting), 
                                                     len(jobs.pending) ))
    if len(jobs.pending) > 1000:
        log.msg("ABORT")
        reactor.callWhenRunning(reactor.stop)
    reactor.callLater(300, job_size, jobs)

def main(_):
    """
    Go main Go!
    """
    log.msg("main() has fired...")
    jobs = DeferredQueue()
    ingest = MyProductIngestor()
    ingest.jobs = jobs
    ldmbridge.LDMProductFactory( ingest )
    
    for _ in range(3):
        cooperate(worker(jobs))
    
    reactor.callLater(0, write_pid)
    reactor.callLater(30, job_size, jobs)

def errback(res):
    ''' ERRORBACK '''
    log.err(res)
    reactor.stop()

df = POSTGISDB.runInteraction(load_station_table)
df.addCallback(main)
df.addErrback( errback )
reactor.run()