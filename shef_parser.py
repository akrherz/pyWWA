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
import access
from support import ldmbridge, TextProduct
import common

# Third Party Stuff
from twisted.enterprise import adbapi
from twisted.internet.defer import DeferredQueue, Deferred
from twisted.internet.task import cooperate
from twisted.internet import reactor, protocol
import mx.DateTime

import ConfigParser
config = ConfigParser.ConfigParser()
config.read(os.path.join(os.path.dirname(__file__), 'cfg.ini'))

# Setup Database Links
ACCESSDB = adbapi.ConnectionPool("twistedpg", database="iem", cp_reconnect=True,
                                host=config.get('database','host'), 
                                user=config.get('database','user'),
                                password=config.get('database','password') )
HADSDB = adbapi.ConnectionPool("twistedpg", database="hads", cp_reconnect=True,
                                host=config.get('database','host'), 
                                user=config.get('database','user'),
                                password=config.get('database','password') )
BASE_TS = mx.DateTime.gmt() - mx.DateTime.RelativeDateTime(months=2)

# Necessary for the shefit program to run A-OK
os.chdir("/home/ldm/pyWWA/shef_workspace")

# Load up our lookup table of stations to networks
LOC2STATE = {}
LOC2NETWORK = {}
UNKNOWN = {}
def load_stations(txn):
    """
    Load up station metadata to help us with writing data to the IEM database
    @param txn database transaction
    """
    txn.execute("""SELECT id, network, state from stations 
        WHERE network ~* 'COOP' or network ~* 'DCP' or network in ('KCCI','KIMT','KELO') 
        ORDER by network ASC""")
    for row in txn:
        stid = row['id']
        LOC2STATE[ stid ] = row['state']
        if LOC2NETWORK.has_key(stid):
            del LOC2NETWORK[stid]
        else:
            LOC2NETWORK[stid] = row['network']

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
             'SFIZ': 'snow',
             'SFDZ': 'snow',
             'SFQZ': 'snow', # This is more instantaneous, or 6hr perhaps
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
        self.tp = TextProduct.TextProduct( buf )
        self.data = ""

    def connectionMade(self):
        """
        Fired when the program starts up and wants stdin
        """
        #print "sending %d bytes!" % len(self.shefdata)
        #print "SENDING", self.shefdata
        self.transport.write( self.tp.raw )
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
        reactor.callLater(0, really_process, self.tp, self.data)
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
        reactor.callLater(5, self.shutdown)

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
        tstamp = mx.DateTime.strptime(dstr, "%Y-%m-%d %H:%M:%S")
        # We don't care about data in the future!
        if tstamp > (mx.DateTime.gmt() + mx.DateTime.RelativeDateTime(hours=1)):
            continue
        if tstamp < BASE_TS:
            log.msg("Rejecting old data %s %s" % (sid, tstamp))
            continue
        if not mydata[sid].has_key(tstamp):
            mydata[sid][tstamp] = {}

        varname = tokens[5]
        value = float(tokens[6])
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

def process_site(tp, sid, ts, data):
    """ 
    I process a dictionary of data for a particular site
    """
    # Insert data into database regardless
    for var in data.keys():
        deffer = HADSDB.runOperation("""INSERT into raw%s 
            (station, valid, key, value) 
            VALUES('%s','%s+00', '%s', '%s')""" % (ts.strftime("%Y_%m"), sid, 
            ts.strftime("%Y-%m-%d %H:%M"), var, 
            data[var]))
        deffer.addErrback(common.email_error, tp)
        deffer.addErrback( log.err )

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

    iemob = access.Ob(sid, network)
    iemob.setObTimeGMT(ts)
    iemob.data['year'] = ts.year

    deffer = ACCESSDB.runInteraction(save_data, tp, iemob, data)
    deffer.addErrback(common.email_error, tp.raw)
    deffer.addCallback(got_results, tp, sid, network)
    deffer.addErrback( log.err )
    
def got_results(res, tp, sid, network):
    """
    Callback after our iemdb work
    @param res response
    @param tp support.TextProduct instance
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
    iemob.txn = txn
    #print data, iemob.data['valid']
    if not iemob.load_and_compare():
        return False
    for var in data.keys():
        if data[var] == -9999:
            continue
        myval = data[var] * MULTIPLIER.get(var[:2], 1.0)
        iemob.data[ MAPPING[var] ] = myval
        if MAPPING[var] == 'tmpf' and iemob.data['network'].find("COOP") > 0:
            iemob.data['coop_tmpf'] = myval
            #print "HEY!", iemob.data['valid'].strftime("%Y-%m-%d %H:%M")
            iemob.data['coop_valid'] = iemob.data['valid'].strftime("%Y-%m-%d %H:%M")
    iemob.data['raw'] = tp.get_product_id()
    iemob.update_summary()
    # Don't go through this pain, unless we need to!
    if 'max_tmpf' in iemob.data.keys():
        iemob.updateDatabaseSummaryTemps()
    iemob.updateDatabase()
    return True

def job_size(jobs):
    """
    Print out some debug information to the log on the current size of the
    job queue
    """
    log.msg("deferredQueue waiting: %s pending: %s" % (len(jobs.waiting), 
                                                     len(jobs.pending) ))
    if len(jobs.pending) > 100:
        log.msg("ABORT")
        reactor.callWhenRunning(reactor.stop)
    reactor.callLater(300, job_size, jobs)

def main():
    """
    Go main Go!
    """
    jobs = DeferredQueue()
    ingest = MyProductIngestor()
    ingest.jobs = jobs
    ldmbridge.LDMProductFactory( ingest )
    
    for i in range(3):
        cooperate(worker(jobs))
    
    reactor.callLater(0, write_pid)
    reactor.callLater(30, job_size, jobs)
    reactor.run()

if __name__ == '__main__':
    HADSDB.runInteraction(load_stations)
    main()
