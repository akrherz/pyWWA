# Copyright (c) 2005-2008 Iowa State University
# http://mesonet.agron.iastate.edu/ -- mailto:akrherz@iastate.edu
#
# This program is free software; you can redistribute it and/or modify it under
# the terms of the GNU General Public License as published by the Free Software
# Foundation; either version 2 of the License, or (at your option) any later
# version.
#
# This program is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
# FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License along with
# this program; if not, write to the Free Software Foundation, Inc.,
# 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
""" 
 SHEF product ingestor 
"""
# System Imports
import os
import datetime

# Setup Standard Logging we use
from twisted.python import log, logfile
log.FileLogObserver.timeFormat = "%Y/%m/%d %H:%M:%S %Z"
log.startLogging(logfile.DailyLogFile('shef_parser.log', 
                                      os.path.abspath('logs/')))

def write_pid():
    """ Create a PID file for when we are fired up! """
    pid = open("shef_parser.pid",'w')
    pid.write("%s" % ( os.getpid(),) )
    pid.close()

# Stuff I wrote
import mesonet
from pyiem.observation import Observation
from pyiem.nws import product
from pyldm import ldmbridge
import common
import iemtz
import pytz

# Third Party Stuff
from twisted.internet import task
from twisted.enterprise import adbapi
from twisted.internet.defer import DeferredQueue, Deferred
from twisted.internet.task import cooperate
from twisted.internet import reactor, protocol

import ConfigParser
config = ConfigParser.ConfigParser()
config.read(os.path.join(os.path.dirname(__file__), 'cfg.ini'))

# Setup Database Links
ACCESSDB_SINGLE = adbapi.ConnectionPool("twistedpg", database="iem", cp_reconnect=True,
                                host=config.get('database','host'), cp_max=1,
                                user=config.get('database','user'),
                                password=config.get('database','password') )
ACCESSDB = adbapi.ConnectionPool("twistedpg", database="iem", cp_reconnect=True,
                                host=config.get('database','host'), cp_max=5,
                                user=config.get('database','user'),
                                password=config.get('database','password') )
HADSDB = adbapi.ConnectionPool("twistedpg", database="hads", cp_reconnect=True,
                                host=config.get('database','host'), 
                                user=config.get('database','user'),
                                password=config.get('database','password') )

# Necessary for the shefit program to run A-OK
os.chdir("%s/shef_workspace" % (os.path.dirname(os.path.abspath(__file__)),))

# Load up our lookup table of stations to networks
LOC2STATE = {}
LOC2NETWORK = {}
LOC2TZ = {}
UNKNOWN = {}
TIMEZONES = {None: pytz.timezone('UTC')}
def load_stations(txn):
    """
    Load up station metadata to help us with writing data to the IEM database
    @param txn database transaction
    """
    txn.execute("""SELECT id, network, state, tzname from stations 
        WHERE network ~* 'COOP' or network ~* 'DCP' or 
        network in ('KCCI','KIMT','KELO') 
        ORDER by network ASC""")
    for row in txn:
        stid = row['id']
        LOC2STATE[ stid ] = row['state']
        LOC2TZ[stid] = row['tzname']
        if LOC2NETWORK.has_key(stid):
            del LOC2NETWORK[stid]
        else:
            LOC2NETWORK[stid] = row['network']
        if not TIMEZONES.has_key(row['tzname']):
            TIMEZONES[ row['tzname'] ] = pytz.timezone( row['tzname'] )

MULTIPLIER = {
  "US" : 0.87,  # Convert MPH to KNT
  "UG": 0.87,
  "UP": 0.87,
  "UR": 10,
}

"""
Some notes on the SHEF codes translated to something IEM Access can handle
First two chars are physical extent code

"""
DIRECTMAP = {
    'URIZ': 'max_drct',
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
        if DIRECTMAP.has_key( pei ):
            self.__setitem__(key, DIRECTMAP[ pei ])
            return DIRECTMAP[ pei ]
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
        self.tp = product.TextProduct( buf )
        self.data = ""

    def connectionMade(self):
        """
        Fired when the program starts up and wants stdin
        """
        #print "sending %d bytes!" % len(self.shefdata)
        #print "SENDING", self.shefdata
        self.transport.write( self.tp.text )
        self.transport.closeStdin()

    def outReceived(self, data):
        """
        Save the stdout we get from the program for later processing
        """
        #print "GOT", data
        self.data = self.data + data

    def errReceived(self, data):
        """
        In case something comes to stderr 
        """
        log.msg("errReceived! with %d bytes!" % len(data))
        log.msg( data )
        self.deferred.errback(data)

#    def processEnded(self, status):
#        print "debug: type(status): %s" % type(status.value)
#        print "error: exitCode: %s" % status.value.exitCode


    def outConnectionLost(self):
        """
        Once the program is done, we need to do something with the data
        """
        #if self.data == "":
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
    return buf.replace("\015\015\012", "\n").replace("\003", "").replace("\001", "")

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
        self.jobs.put( clnstr(buf) )

def async(buf):
    """
    Async caller of reactor processes
    @param buf string of the raw NOAAPort Product
    """
    defer = Deferred()
    proc = SHEFIT(buf)
    proc.deferred = defer
    proc.deferred.addErrback( log.err )

    reactor.spawnProcess(proc, "shefit", 
                   ["shefit"], {})
    return proc.deferred


def worker(jobs):
    while True:
        yield jobs.get().addCallback(async)

def really_process(tp, data):
    """
    This processes the output we get from the SHEFIT program
    """
    # Now we loop over the data we got :)
    #log.msg("\n"+data)
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
        if not mydata.has_key(sid):
            mydata[sid] = {}
        dstr = "%s %s" % (tokens[1], tokens[2])
        tstamp = datetime.datetime.strptime(dstr, "%Y-%m-%d %H:%M:%S")
        tstamp = tstamp.replace(tzinfo=iemtz.UTC())
        # We don't care about data in the future!
        utcnow = datetime.datetime.utcnow().replace(tzinfo=iemtz.UTC())
        if tstamp > (utcnow + datetime.timedelta(hours=1)):
            continue
        if tstamp < (utcnow - datetime.timedelta(days=60)):
            log.msg("Rejecting old data %s %s" % (sid, tstamp))
            continue
        if not mydata[sid].has_key(tstamp):
            mydata[sid][tstamp] = {}

        varname = tokens[5]
        value = float(tokens[6])
        if varname[:2] in ['TV','TB']: # Soil Depth fun!
            depth = int(value)
            value = abs( (value * 1000) % (depth * 1000) )
            if depth < 0:
                value = 0 - value
                depth = abs(depth)
            varname = "%s.%02i" % (varname, depth)
                
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
    #log.msg("Found unknown %s %s %s" % (sid, tp.get_product_id(), network))
    HADSDB.runOperation("""
            INSERT into unknown(nwsli, product, network) 
            values ('%s', '%s', '%s')
        """ % (sid, tp.get_product_id() , network))

def checkvars( myvars ):
    """
    Check variables to see if we have a COOP or DCP site
    """
    for v in myvars:
        # Definitely DCP
        if v[:2] in ['HG',]:
            return False
        if v[:2] in ['SF','SD']:
            return True
        if v[:3] in ['PPH',]:
            return False
    return False

def var2dbcols(var):
    """ Convert a SHEF var into split values """
    if var.find(".") > -1:
        parts = var.split(".")
        var = parts[0]
        return [ var[:2], var[2], var[3:5], var[5], var[-1], parts[1] ]
    else:
        return [ var[:2], var[2], var[3:5], var[5], var[-1], None ]

    

def process_site(tp, sid, ts, data):
    """ 
    I process a dictionary of data for a particular site
    """
    localts = ts.astimezone( TIMEZONES[LOC2TZ.get(sid) ])
    #log.msg("%s sid: %s ts: %s %s" % (tp.get_product_id(), sid, ts, localts))
    # Insert data into database regardless
    for varname in data.keys():
        value = data[varname]
        deffer = HADSDB.runOperation("""INSERT into raw"""+
                                      ts.strftime("%Y_%m") +""" 
            (station, valid, key, value) 
            VALUES(%s,%s, %s, %s)""", ( sid, 
            ts.strftime("%Y-%m-%d %H:%M+00"), varname, value))
        deffer.addErrback(common.email_error, tp.text)
        deffer.addErrback( log.err )

        (pe, d, s, e, p, depth) = var2dbcols(varname)
        d2 = ACCESSDB_SINGLE.runOperation("""
        INSERT into current_shef(station, valid, physical_code, duration,
        source, extremum, probability, value, depth) 
        values (%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """, (sid, ts.strftime("%Y-%m-%d %H:%M+00"), pe, d, s, e, p, value,
              depth))
        d2.addErrback( common.email_error, tp.text )

    # Our simple determination if the site is a COOP site
    is_coop = False
    if tp.afos[:3] == 'RR3':
        is_coop = True
    elif tp.afos[:3] in ['RR1', 'RR2'] and checkvars( data.keys() ):
        log.msg("Guessing COOP? %s %s %s" %  (sid, tp.get_product_id(), 
                                              data.keys()))
        is_coop = True


    state = LOC2STATE.get( sid )
    if state is None and len(sid) == 8 and sid[0] == 'X':
        return
    # Base the state on the 2 char station ID!
    if state is None and len(sid) == 5 and mesonet.nwsli2state.has_key(sid[3:]):
        state = mesonet.nwsli2state.get(sid[3:])
        LOC2STATE[sid] = state
    if state is None:
        if UNKNOWN.get(sid) is None:
            enter_unknown(sid, tp, "")
            UNKNOWN[sid] = 1       
        return 
    
    # Deterime if we want to waste the DB's time
    network = LOC2NETWORK.get(sid)
    if network in ['KCCI', 'KIMT', 'KELO']:
        return
    if network is None:
        if is_coop:
            network = "%s_COOP" % (state,)
        # We are left with DCP
        else:
            network = "%s_DCP" % (state,)

    # Do not send DCP sites to IEMAccess
    if network.find("_DCP") > 0:
        return

    # Okay, time for a hack, if our observation is at midnight!
    if localts.hour == 0 and localts.minute == 0:
        localts -= datetime.timedelta(minutes=1)
        log.msg("Shifting %s [%s] back one minute: %s" % (sid, network, 
                                                          localts))

    iemob = Observation(sid, network, localts)

    deffer = ACCESSDB.runInteraction(save_data, tp, iemob, data)
    deffer.addErrback(common.email_error, tp.text)
    deffer.addCallback(got_results, tp, sid, network)
    deffer.addErrback( log.err )
    
def got_results(res, tp, sid, network):
    """
    Callback after our iemdb work
    @param res response
    @param tp pyiem.nws.product.TextProduct instance
    @param sid stationID
    @param network string network
    """
    if not res:
        if UNKNOWN.get(sid) is None:
            enter_unknown(sid, tp, network)
            UNKNOWN[sid] = 1
    
def save_data(txn, tp, iemob, data):
    """
    Called from a transaction 'thread'
    """

    for var in data.keys():
        if data[var] == -9999:
            continue
        myval = data[var] * MULTIPLIER.get(var[:2], 1.0)
        iemob.data[ MAPPING[var] ] = myval
        if MAPPING[var] == 'tmpf' and iemob.data['network'].find("COOP") > 0:
            iemob.data['coop_tmpf'] = myval
            #print "HEY!", iemob.data['valid'].strftime("%Y-%m-%d %H:%M")
            iemob.data['coop_valid'] = iemob.data['valid']
    iemob.data['raw'] = tp.get_product_id()
    return iemob.save(txn)

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

def main(res):
    """
    Go main Go!
    """
    log.msg("main() fired!")
    jobs = DeferredQueue()
    ingest = MyProductIngestor()
    ingest.jobs = jobs
    ldmbridge.LDMProductFactory( ingest )
    
    for i in range(3):
        cooperate(worker(jobs))
    
    reactor.callLater(0, write_pid)
    reactor.callLater(300, job_size, jobs)

def fullstop(err):
    log.msg("fullstop() called...")
    log.err( err )
    reactor.stop()    

if __name__ == '__main__':
    df = HADSDB.runInteraction(load_stations)
    df.addCallback( main )
    df.addErrback( fullstop )
    reactor.run()
