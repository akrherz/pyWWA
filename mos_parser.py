# Copyright (c) 2005 Iowa State University
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
""" MOS Data Ingestor, why not? """

__revision__ = '$Id: mos_parset.py 3802 2008-07-29 19:55:56Z akrherz $'

from twisted.python import log
from twisted.python import logfile
log.FileLogObserver.timeFormat = "%Y/%m/%d %H:%M:%S %Z"
log.startLogging( logfile.DailyLogFile('mos_parser.log', 'logs') )

# Twisted Python imports
from twisted.internet import reactor
from twisted.enterprise import adbapi

# Standard Python modules
import re

# Python 3rd Party Add-Ons
import mx.DateTime

# pyWWA stuff
from support import ldmbridge
import common
import ConfigParser

config = ConfigParser.ConfigParser()
config.read("/home/ldm/pyWWA/cfg.ini")

DBPOOL = adbapi.ConnectionPool("twistedpg", database="mos", 
                               host=config.get('database','host'), 
                               user=config.get('database','user'),
                               password=config.get('database','password'), 
                               cp_reconnect=True)

class myProductIngestor(ldmbridge.LDMProductReceiver):

    def process_data(self, buf):
        """
        Actual ingestor
        """
        try:
            real_process(buf)
        except Exception, myexp:
            common.email_error(myexp, buf)

    def connectionLost(self, reason):
        """
        Called when ldm closes the pipe
        """
        print 'connectionLost', reason
        reactor.callLater(5, self.shutdown)
    
    def shutdown(self):
        reactor.callWhenRunning(reactor.stop)



def real_process(raw):
    """ The real processor of the raw data, fun! """
    raw += "\015\012"
    raw = raw.replace("\015\015\012", "___").replace("\x1e", "")
    sections = re.findall("([A-Z0-9]{4}\s+... MOS GUIDANCE .*?)______", raw)
    map(section_parser, sections)
    if len(sections) == 0:
        common.email_error("FAILED REGEX", raw)

def section_parser(sect):
    """ Actually process a data section, getting closer :) """
    metadata = re.findall("([A-Z0-9]{4})\s+(...) MOS GUIDANCE\s+([01]?[0-9])/([0-3][0-9])/([0-9]{4})\s+([0-2][0-9]00) UTC", sect)
    (station, model, month, day, year, hhmm) = metadata[0]
    initts = mx.DateTime.DateTime(int(year), int(month), int(day), int(hhmm[:2]))
    
    times = [initts,]
    data = {}
    lines = sect.split("___")
    hrs = lines[2].split()
    for h in hrs[1:]:
        if (h == "00"):
            ts = times[-1] + mx.DateTime.RelativeDateTime(days=1, hour=0)
        else:
            ts = times[-1] + mx.DateTime.RelativeDateTime(hour=int(h))
        times.append( ts )
        data[ts] = {}

    for line in lines[3:]:
        if (len(line) < 10):
            continue
        vname = line[:3].replace("/","_")
        if (vname == "X_N"):
            vname = "N_X"
        vals = re.findall("(...)", line[4:])
        for i in range(len(vals)):
            if vname == "T06" and times[i+1].hour in [0, 6, 12, 18]:
                data[ times[i+1] ]["T06_1"] = vals[i-1].replace("/","").strip()
                data[ times[i+1] ]["T06_2"] = vals[i].replace("/","").strip()
            elif (vname == "T06"):
                pass
            elif vname == "T12" and times[i+1].hour in [0, 12]:
                data[ times[i+1] ]["T12_1"] = vals[i-1].replace("/","").strip()
                data[ times[i+1] ]["T12_2"] = vals[i].replace("/","").strip()
            elif (vname == "T12"):
                pass
            elif (vname == "WDR"):
                data[ times[i+1] ][ vname ] = int(vals[i].strip()) * 10
            else:
                data[ times[i+1] ][ vname ] = vals[i].strip()

    inserts = 0
    for ts in data.keys():
        if (ts == initts):
            continue
        fst = "INSERT into t%s (station, model, runtime, ftime," % (initts.year,)
        sst = "VALUES('%s','%s','%s+00','%s+00'," % (station, model, initts, ts)
        for vname in data[ts].keys():
            fst += " %s," % (vname,)
            sst += " '%s'," % (data[ts][vname],)
        sql = fst[:-1] +") "+ sst[:-1] +")"
        deffer = DBPOOL.runOperation( sql.replace("''", "Null") )
        deffer.addErrback( common.email_error, sql)
        inserts += 1
    # Simple debugging
    if inserts == 0:
        common.email_error("No data found?", sect)

ldm = ldmbridge.LDMProductFactory( myProductIngestor() )
reactor.run()
