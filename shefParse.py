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
""" SHEF product ingestor """


import sys, sys, os
import traceback
from pyIEM import shefReport, iemAccessOb, mesonet, iemdb
from support import ldmbridge
from twisted.python import log
from twisted.internet import reactor
import smtplib, StringIO
from email.MIMEText import MIMEText
import secret

log.startLogging(open('logs/shefParse.log', 'a')
log.FileLogObserver.timeFormat = "%Y/%m/%d %H:%M:%S %Z"

from twisted.enterprise import adbapi
dbpool = adbapi.ConnectionPool("psycopg2", database='iem', host=secret.dbhost)
i = iemdb.iemdb()
iemaccess = i['iem']

multiplier = {
  "US" : 0.87,  # Convert MPH to KNT
  "USIRG": 0.87,
  "UG": 0.87,
  "UGIRG": 0.87,
  "UP": 0.87,
  "UPIRG": 0.87,
  "UPVRG": 0.87,
}

mapping = {
  "HGIRZ": "rstage",
  "HGIRG": "rstage",
  "HG": "rstage",
  "PPHRG": "phour", "PPH": "phour",
  "raw": "raw",
  "TAIRG": "tmpf",
  "TA": "tmpf", "TD": "dwpf",
  -1: "", -2: "", -3: "", -4: "", -5: "",
  "TX": "max_tmpf",
  "TN": "min_tmpf",
  "SD": "snowd", "XR": "relh",
  "PP": "pday", "PA": "pres", "PPD": "pday",
  "SW": "snoww", "USIRG": "sknt",
  "SF": "snow", "UD": "drct", "UG": "gust", "UDIRG": "drct",
  "UPIRG": "gust", "UPVRG": "gust",
  "PCIRG": "", "PPCRG": "", "TWIRG": "", "VBIRG": "", "HTIRG": "",
  "HPIRG": "", "PPDRG": "", "PPQRG": "", "PPDRP": "", "PPQRP": "",
  "AD": "", "SFK": "", "UP": "", "US": "sknt", "UR": "", "PPQ": "",
  "EP": "", "SFQ": "",
}
mystates = ['IA', 'ND','SD','NE','KS','MO','MN','WI','IL','IN','OH','MI']
EMAILS = 10

class myProductIngestor(ldmbridge.LDMProductReceiver):


  def connectionLost(self, reason):
    log.msg("LDM Closed PIPE")

  def process_data(self, buf):
    cstr = StringIO.StringIO()
    try:
      self.real_process(buf, cstr)
    except:
      traceback.print_exc(file=cstr)
      cstr.seek(0)
      errstr = cstr.read()
      #log.msg("_________ERROR__________")
      log.msg( errstr )
      log.msg("|%s|" % (buf.replace("\015\015\012", "\n"),))
      #log.msg("_________END__________")
      msg = MIMEText("%s\n\n>RAW DATA\n\n%s" % (errstr,
           buf.replace("\015\015\012", "\n") ) )
      msg['subject'] = 'shefParse.py Traceback'
      msg['From'] = secret.parser_user
      msg['To'] = "akrherz@iastate.edu"
      global EMAILS
      if (EMAILS > 0):
        smtp = smtplib.SMTP()
        smtp.connect()
        smtp.sendmail(msg["From"], msg["To"], msg.as_string())
        smtp.close()
      EMAILS -= 1
      log.msg("EMAILS [%s]" % (EMAILS,))

  def real_process(self, buf, cstr):
    sreport = shefReport.shefReport(buf, cstr )

    for sid in sreport.db.keys():  # Get all stations in this report
      # Prevent Cross Pollenation for now?
      if (len(sid) != 5):
        continue
      isCOOP = 0
      if (not mesonet.nwsli2state.has_key(sid[-2:])):
        print "UNKNOWN SID STATE CODE [%s]" % (sid,)
        continue
      state = mesonet.nwsli2state[ sid[-2:]]
      # We need to sort the times, so that we don't process old data?
      times = sreport.db[sid].keys()
      times.sort()
      for ts in times:  # Each Time
        if (ts == 'writets'):
          continue

        # Loop thru vars to see if we have a COOP site?
        for var in sreport.db[sid][ts].keys():
          if (var in ['TX','TN','PP']):
            isCOOP = 1
          if (not mapping.has_key(var)):
            print "Couldn't map var", var
            mapping[var] = ""
          if (not multiplier.has_key(var)):
            multiplier[var] = 1.0


        # Deterime if we want to waste the DB's time
        # If COOP in MW, process it
        if (isCOOP and state in mystates):
          print "FIND COOP?", sid, ts
          network = state+"_COOP"
        # We are left with DCPs in Iowa
        elif (state == "IA"):
          network = "DCP"
        # Everybody else can go away :)
        else:
          continue

        # Lets do this, finally.
        iemob = iemAccessOb.iemAccessOb(sid, network)
        iemob.data['ts'] = ts
        iemob.data['year'] = ts.year
        iemob.load_and_compare(iemaccess)
        for var in sreport.db[sid][ts].keys():
          if sreport.db[sid][ts][var] is None:
            continue
          if (var == "raw"):
            iemob.data[ mapping[var] ] = sreport.db[sid][ts][var]
          else:
            iemob.data[ mapping[var] ] = cleaner(sreport.db[sid][ts][var]) * multiplier[var]

        iemob.updateDatabaseSummaryTemps(None, dbpool)
        iemob.updateDatabase(None, dbpool)
        del iemob

def cleaner(v):
  if (v == "" or v is None or v == "M"):
    return -99
  if (v.upper() == "T"):
    return 0.0001
  return float(v.replace("F","").replace("Q",""))

#
fact = ldmbridge.LDMProductFactory( myProductIngestor() )
reactor.run()
