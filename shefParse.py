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
from pyIEM import shefReport, iemAccessOb, mesonet
from support import ldmbridge
from twisted.python import log
from twisted.internet import reactor
import smtplib, StringIO
from email.MIMEText import MIMEText
import secret

log.startLogging(open('/mesonet/data/logs/%s/shefParse.log' \
    % (os.getenv("USER"),), 'a'))
log.FileLogObserver.timeFormat = "%Y/%m/%d %H:%M:%S %Z"

from twisted.enterprise import adbapi
dbpool = adbapi.ConnectionPool("psycopg2", database='iem', host=secret.dbhost)

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
  "SD": "snowd",
  "PP": "pday",
  "SW": "snoww", "USIRG": "sped",
  "SF": "snow", "UD": "drct", "UG": "gust",
  "PCIRG": "", "PPCRG": "", "TWIRG": "", "VBIRG": "", "HTIRG": "",
  "HPIRG": "", "PPDRG": "", "PPQRG": "", "PPDRP": "", "PPQRP": "",
  "AD": "", "SFK": "", "UP": "", "US": "sped", "UR": "", "PPQ": "",
  "EP": "", "SFQ": "",
}

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
      

  def real_process(self, buf, cstr):
    sreport = shefReport.shefReport(buf, cstr )

    for sid in sreport.db.keys():  # Get all stations in this report
      # Prevent Cross Pollenation for now?
      if (len(sid) != 5):
        continue
      isCOOP = 0
      state = mesonet.nwsli2state[ sid[-2:]]
      for ts in sreport.db[sid].keys():  # Each Time
        if (ts == 'writets'):
          continue
        iemob = iemAccessOb.iemAccessOb(sid)
        iemob.data['ts'] = ts
        iemob.data['year'] = ts.year
        for var in sreport.db[sid][ts].keys():
          if (var in ['TX','TN','PP']):
            isCOOP = 1
          if (mapping.has_key(var)):
            iemob.data[ mapping[var] ] = cleaner(sreport.db[sid][ts][var])
          else:
            print "Couldn't map var", var
            mapping[var] = ""
        if (isCOOP):
          print "FIND COOP?", sid, ts
          iemob.set_network(state+"_COOP")
        elif (state == "IA"):
          iemob.set_network("DCP")
        else:
          del iemob
          continue
        iemob.updateDatabaseSummaryTemps(None, dbpool)
        iemob.updateDatabase(None, dbpool)
        del iemob

def cleaner(v):
  if (v == "" or v is None or v == "M"):
    return -99
  if (v.upper() == "T"):
    return 0.0001
  return v.replace("F","")

#
fact = ldmbridge.LDMProductFactory( myProductIngestor() )
reactor.run()
