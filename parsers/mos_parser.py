""" MOS Data Ingestor, why not? """

# Twisted Python imports
from syslog import LOG_LOCAL2
from twisted.python import syslog
syslog.startLogging(prefix='pyWWA/mos_parser', facility=LOG_LOCAL2)
from twisted.python import log


# Twisted Python imports
from twisted.internet import reactor

# Standard Python modules
import re

import datetime
import pytz

# pyWWA stuff
from pyldm import ldmbridge
import common

DBPOOL = common.get_database('mos')

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



def real_process(initial_raw):
    """ The real processor of the raw data, fun! """
    raw = initial_raw + "\015\012"
    raw = raw.replace("\015\015\012", "___").replace("\x1e", "")
    sections = re.findall("([A-Z0-9]{4}\s+... MOS GUIDANCE .*?)______", raw)
    map(section_parser, sections)
    if len(sections) == 0:
        common.email_error("FAILED REGEX", initial_raw)

def make_null(v):
    if v == "" or v is None:
        return None
    return v

def section_parser(sect):
    """ Actually process a data section, getting closer :) """
    metadata = re.findall("([A-Z0-9]{4})\s+(...) MOS GUIDANCE\s+([01]?[0-9])/([0-3][0-9])/([0-9]{4})\s+([0-2][0-9]00) UTC", sect)
    (station, model, month, day, year, hhmm) = metadata[0]
    initts = datetime.datetime(int(year), int(month), int(day), int(hhmm[:2]))
    initts = initts.replace(tzinfo=pytz.timezone("UTC"))
    
    times = [initts,]
    data = {}
    lines = sect.split("___")
    hrs = lines[2].split()
    for h in hrs[1:]:
        if (h == "00"):
            ts = times[-1] + datetime.timedelta(days=1)
            ts = ts.replace(hour=0)
        else:
            ts = times[-1].replace(hour=int(h))
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
        sst = "VALUES(%s,%s,%s,%s," 
        args = [station, model, initts, ts]
        for vname in data[ts].keys():
            fst += " %s," % (vname,)
            sst += "%s," 
            args.append( make_null(data[ts][vname]) )
        sql = fst[:-1] +") "+ sst[:-1] +")"
        deffer = DBPOOL.runOperation( sql, args )
        deffer.addErrback( common.email_error, sect)
        inserts += 1
    # Simple debugging
    if inserts == 0:
        common.email_error("No data found?", sect)

ldm = ldmbridge.LDMProductFactory( myProductIngestor() )
reactor.run()
