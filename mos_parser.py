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

# Twisted Python imports
from twisted.internet import reactor
from twisted.python import log
from twisted.enterprise import adbapi

# Standard Python modules
import os, re, traceback, StringIO, smtplib
from email.MIMEText import MIMEText

# Python 3rd Party Add-Ons
import mx.DateTime, pg, psycopg2

# pyWWA stuff
from support import ldmbridge, TextProduct
import secret
import common

log.startLogging(open('logs/mos_parser.log', 'a'))
log.FileLogObserver.timeFormat = "%Y/%m/%d %H:%M:%S %Z"


DBPOOL = adbapi.ConnectionPool("psycopg2", database="mos", host=secret.dbhost, password=secret.dbpass)

class myProductIngestor(ldmbridge.LDMProductReceiver):

    def process_data(self, buf):
        try:
            real_process(buf)
        except Exception,myexp:
            email_error(myexp, buf)

    def connectionLost(self,reason):
        log.msg(reason)
        log.msg("LDM Closed PIPE")


def real_process(raw):
    """ The real processor of the raw data, fun! """
    raw += "\015\012"
    raw = raw.replace("\015\015\012", "___").replace("\x1e", "")
    sections = re.findall("([A-Z0-9]{4}\s+... MOS GUIDANCE .*?)______", raw)
    map(section_parser, sections)

def section_parser(sect):
    """ Actually process a section, getting closer :) """
    metadata = re.findall("([A-Z0-9]{4})\s+(...) MOS GUIDANCE\s+([01][0-9])/([0-3][0-9])/([0-9]{4})\s+([0-2][0-9]00) UTC", sect)
    (station, model, month, day, year, hhmm) = metadata[0]
    initts = mx.DateTime.DateTime(int(year), int(month), int(day), int(hhmm[:2]))
    print "PROCESS", station, model, initts

    times = [initts,]
    data = {}
    lines = sect.split("___")
    hrs = lines[2].split()
    for h in hrs[1:]:
      if (h == "00"):
        ts = times[-1] + mx.DateTime.RelativeDateTime(days=1,hour=0)
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
        if vname == "T06" and [0,6,12,18].__contains__(times[i+1].hour):
          data[ times[i+1] ]["T06_1"] = vals[i-1].replace("/","").strip()
          data[ times[i+1] ]["T06_2"] = vals[i].replace("/","").strip()
        elif (vname == "T06"):
          pass
        elif vname == "T12" and [0,12].__contains__(times[i+1].hour):
          data[ times[i+1] ]["T12_1"] = vals[i-1].replace("/","").strip()
          data[ times[i+1] ]["T12_2"] = vals[i].replace("/","").strip()
        elif (vname == "T12"):
          pass
        elif (vname == "WDR"):
          data[ times[i+1] ][ vname ] = int(vals[i].strip()) * 10
        else:
          data[ times[i+1] ][ vname ] = vals[i].strip()

    for ts in data.keys():
      if (ts == initts):
        continue
      fst = "INSERT into t%s (station, model, runtime, ftime," % (initts.year,)
      sst = "VALUES('%s','%s','%s+00','%s+00'," % (station, model, initts, ts)
      for vname in data[ts].keys():
        fst += " %s," % (vname,)
        sst += " '%s'," % (data[ts][vname],)
      sql = fst[:-1] +") "+ sst[:-1] +")"
      DBPOOL.runOperation( sql.replace("''", "Null") ).addErrback( email_error, sql)

EMAILS = 0
def email_error(message, product_text):
    """
    Generic something to send email error messages 
    """
    global EMAILS
    log.msg( message )
    EMAILS -= 1
    if (EMAILS < 0):
        return

    msg = MIMEText("Exception:\n%s\n\nRaw Product:\n%s" \
                 % (message, product_text))
    msg['subject'] = 'mos_parser.py Traceback'
    msg['From'] = secret.parser_user
    msg['To'] = 'akrherz@iastate.edu'
    smtp = smtplib.SMTP()
    smtp.connect()
    smtp.sendmail(msg['From'], msg['To'], msg.as_string())
    smtp.close()


ldm = ldmbridge.LDMProductFactory( myProductIngestor() )
reactor.run()
