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
  Take the NCR NEXRAD Level III product and run it through gpnids to get the
  attribute table, which we then dump into the database 
"""
# System Imports
import os
import math

# Setup Standard Logging we use
from twisted.python import log, logfile
log.FileLogObserver.timeFormat = "%Y/%m/%d %H:%M:%S %Z"
log.startLogging(logfile.DailyLogFile('nexrad3_attr.log', 
                                      os.path.abspath('logs/')))

# Need to do this in order to get the subsequent calls to work, TODO
os.chdir("/home/ldm/pyWWA")

# Stuff I wrote
from support import ldmbridge, stationTable

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
POSTGISDB = adbapi.ConnectionPool("twistedpg", database="postgis", cp_reconnect=True,
                                host=config.get('database','host'), 
                                user=config.get('database','user'),
                                password=config.get('database','password') )

ST = stationTable.stationTable('/home/ldm/pyWWA/tables/nexrad.stns')

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

    gmt = mx.DateTime.gmt() + mx.DateTime.RelativeDateTime(hour=hour,
                                                           minute=minute,
                                                           second=0)
    if gmt.day > 25 and day == 1: # Next month!
        gmt += mx.DateTime.RelativeDateTime(days=15) # careful
    gmt += mx.DateTime.RelativeDateTime(day=day)    
        
    return gmt

class PROC(protocol.ProcessProtocol):
    """
    My process protocol 
    """

    def __init__(self, buf):
        """
        Constructor
        """
        #log.msg("init() of PROC")
        lines = buf.split("\r\r\n")
        if len(lines) < 4:
            log.msg("INCOMPLETE PRODUCT!")
            return
        self.wmo = lines[2]
        self.afos = lines[3]
        self.buf = buf
        self.ts = compute_ts( self.wmo )
        self.res = ""
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
        self.deferred.errback(data)



    def outConnectionLost(self):
        """
        Once the program is done, we need to do something with the data
        """
        #log.msg("Teardown")
        if self.res.find("NO STORMS DETECTED") > 0:
            self.deferred.callback(self)
            return
        POSTGISDB.runInteraction(really_process, self.res, self.afos[3:], 
                                 self.ts)
        #log.msg("done!")
        self.deferred.callback(self)
        

class MyProductIngestor(ldmbridge.LDMProductReceiver):
    
    def connectionLost(self, reason):
        log.msg('connectionLost')
        log.msg(reason)
        reactor.callLater(15, self.shutdown)

    def shutdown(self):
        reactor.callWhenRunning(reactor.stop)

    def process_data(self, buf):
        """
        I am called from the ldmbridge when data is ahoy
        """
        self.jobs.put( buf )

def async(buf):
    """
    Async caller of reactor processes
    @param buf string of the raw NOAAPort Product
    """
    defer = Deferred()
    proc = PROC(buf)
    proc.deferred = defer
    proc.deferred.addErrback( log.err )

    log.msg("PROCESS %s %s" % (proc.afos, proc.ts.strftime("%Y%m%d%H%M") ))
    
    reactor.spawnProcess(proc, "python", 
                   ["python", "ncr2postgis.py", proc.afos[3:],
                    proc.ts.strftime("%Y%m%d%H%M")], {})
    return proc.deferred

def worker(jobs):
    """ I am a worker that processes jobs """
    while True:
        yield jobs.get().addCallback(async)

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
    
    """
    txn.execute("DELETE from nexrad_attributes WHERE nexrad = '%s'" % (nexrad) )

    cenlat = float(ST.sts[nexrad]['lat'])
    cenlon = float(ST.sts[nexrad]['lon'])
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
            tokens.insert(5, 0)
            tokens.insert(5, 0)
            tokens.insert(5, 0)
        if len(tokens) < 13:
            log.msg("Incomplete Line ||%s||" % (line,))
            continue
        d = {}
        co += 1
        d["storm_id"] = tokens[0]
        d["azimuth"] = float(tokens[1])
        d["range"] = float(tokens[2]) * 1.852
        d["tvs"] = tokens[3]
        d["meso"] = tokens[4]
        d["posh"] = tokens[5]
        d["poh"] = tokens[6]
        if (tokens[7] == "<0.50"):
            tokens[7] = 0.01
        d["max_size"] = tokens[7]
        d["vil"] = tokens[8]
        d["max_dbz"] = tokens[9]
        d["max_dbz_height"] = tokens[10]
        d["top"] = tokens[11]
        if tokens[12] == "NEW":
            d["drct"], d["sknt"] = 0,0
        else:
            d["drct"] = tokens[12]
            d["sknt"] = tokens[13]
        d["nexrad"] = nexrad

        cosaz = math.cos( d["azimuth"] * math.pi / 180.0 )
        sinaz = math.sin( d["azimuth"] * math.pi / 180.0 )
        mylat = cenlat + (cosaz * (d["range"] * 1000.0) / latscale)
        mylon = cenlon + (sinaz * (d["range"] * 1000.0) / lonscale)
        d["geom"] = "SRID=4326;POINT(%s %s)" % (mylon, mylat)
        d["valid"] = ts.strftime("%Y-%m-%d %H:%M+00")

        for table in ['nexrad_attributes', 'nexrad_attributes_log']:
            sql = """INSERT into """+table+""" (nexrad, storm_id, geom, azimuth,
    range, tvs, meso, posh, poh, max_size, vil, max_dbz, max_dbz_height,
    top, drct, sknt, valid) values (%(nexrad)s, %(storm_id)s, %(geom)s,
    %(azimuth)s, %(range)s, %(tvs)s, %(meso)s, %(posh)s,
    %(poh)s, %(max_size)s, %(vil)s, %(max_dbz)s,
    %(max_dbz_height)s, %(top)s, %(drct)s, %(sknt)s, %(valid)s)"""
            txn.execute( sql, d )

    if co == 0:
        log.msg("Got zero entries ||%s||" % (res,))
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
    main()
