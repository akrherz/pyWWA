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
from twisted.internet import reactor, utils, protocol
import smtplib, StringIO
from email.MIMEText import MIMEText
import secret, mx.DateTime

log.startLogging(open('logs/shefParse.log', 'a'))
log.FileLogObserver.timeFormat = "%Y/%m/%d %H:%M:%S %Z"

from twisted.enterprise import adbapi
dbpool = adbapi.ConnectionPool("psycopg2", database='iem', host=secret.dbhost)
dbpool2 = adbapi.ConnectionPool("psycopg2", database='hads', host=secret.dbhost)
i = iemdb.iemdb()
iemaccess = i['iem']

os.chdir("/home/ldm/pyWWA/shef_workspace")

multiplier = {
  "US" : 0.87,  # Convert MPH to KNT
  "USIRG": 0.87,
  "USIRZZ": 0.87,
  "UG": 0.87,
  "UGIRG": 0.87,
  "UGIRZZ": 0.87,
  "UP": 0.87,
  "UPIRG": 0.87,
  "UPIRZZ": 0.87,
  "UPVRG": 0.87,
  "UPVRZZ": 0.87,
}

mapping = {
  "HGIRZ": "rstage",
  "HGIRG": "rstage",
  "HG": "rstage",

  "PPHRG": "phour", 
  "PPH": "phour",

  "TD": "dwpf",
 
  "TAIRG": "tmpf",
  "TAIRZZ": "tmpf", 
  "TA": "tmpf",

  "TAIRZNZ": "min_tmpf", 
  "TN": "min_tmpf",

  "TAIRZXZ": "max_tmpf",
  "TX": "max_tmpf",

  "PPDRZZ": "pday",
  "PPD": "pday",
  "PP": "pday",
 
  "SD": "snowd", 
  "SDIRZZ": "snowd", 
  "SDIRGZ": "snowd", 
 
  "XR": "relh",

  "PA": "pres", 

  "SW": "snoww",
  "SWIRZZ": "snoww",

  "USIRG": "sknt",
  "US": "sknt",
  "USIRZZ": "sknt",
 
  "SF": "snow",
  "SFDRZZ": "snow",

  "UD": "drct", 
  "UDIRG": "drct",
  "UDIRZZ": "drct",

  "UG": "gust", 
  "UGIRZZ": "gust",

  "UPIRG": "max_sknt",
  "UPVRG": "max_sknt",
  "UPIRZZ": "max_sknt",

  "URIRZZ": "max_drct",
}
mystates = ['IA', 'ND','SD','NE','KS','MO','MN','WI','IL','IN','OH','MI']
EMAILS = 10

class SHEFIT(protocol.ProcessProtocol):

  def __init__(self,shefdata):
    self.shefdata = shefdata
    self.data = ""

  def connectionMade(self):
    #print "SENDING", self.shefdata
    self.transport.write(self.shefdata)
    self.transport.closeStdin()

  def outReceived(self, data):
    #print "GOT", data
    self.data = self.data + data


  def outConnectionLost(self):
    really_process(self.data)


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
    s = SHEFIT(buf.replace("\003", "").replace("\001", "").replace("\015\015\012", "\n") )
    reactor.spawnProcess(s, "shefit", ["shefit"], {})

def i_want_site(sid):
  """ Return bool if I want to actually process this site """
  if (len(sid) != 5):
    return 0
  if (not mesonet.nwsli2state.has_key(sid[-2:])):
    return 0

  state = mesonet.nwsli2state[ sid[-2:]]
  if (not state in mystates):
    return 0

  return 1

def really_process(data):
  # Now we loop over the data we got :)
  mydata = {}
  for line in data.split("\n"):
    tokens = line.split()
    if len(tokens) < 7:
      continue
    sid = tokens[0]
    if (not i_want_site(sid)):
      #print "I DONT WANT", sid
      continue
    if (not mydata.has_key(sid)):
      mydata[sid] = {}
    try:
      ts = mx.DateTime.strptime("%s %s" % (tokens[1], tokens[2]), "%Y-%m-%d %H:%M:%S")
    except:
      print "DateParse error", tokens
      return
    if (not mydata[sid].has_key(ts)):
      mydata[sid][ts] = {}
    v = tokens[5]
    val = tokens[6]
    mydata[sid][ts][v] = val

  for sid in mydata.keys():  # Get all stations in this report
    isCOOP = 0
    state = mesonet.nwsli2state[ sid[-2:]]
    # We need to sort the times, so that we don't process old data?
    times = mydata[sid].keys()
    times.sort()
    for ts in times:  # Each Time
      #print sid, ts, mydata[sid][ts].keys()
      # Loop thru vars to see if we have a COOP site?
      for var in mydata[sid][ts].keys():
        if (var in ['TAIRZZ','TAIRZNZ','TAIRZXZ', 'PPDRZZ']):
          isCOOP = 1
        if (not mapping.has_key(var)):
          print "Couldn't map var: %s for SID: %s" % (var, sid)
          mapping[var] = ""
        if (not multiplier.has_key(var)):
          multiplier[var] = 1.0
        dbpool2.runOperation("INSERT into raw%s(station, valid, key, value) \
          VALUES('%s','%s+00', '%s', '%s')" % (ts.year, sid, \
          ts.strftime("%Y-%m-%d %H:%M"), var, mydata[sid][ts][var]))

      #print sid, state, isCOOP
      # Deterime if we want to waste the DB's time
      # If COOP in MW, process it
      if (isCOOP):
        print "FIND COOP?", sid, ts, mydata[sid][ts].keys()
        network = state+"_COOP"
      # We are left with DCPs in Iowa
      elif (state == "IA"):
        network = "DCP"
      # Everybody else can go away :)
      else:
        continue

      # Lets do this, finally.
      #print "ACTUALLY PROCESSED", sid, network
      iemob = iemAccessOb.iemAccessOb(sid, network)
      iemob.data['ts'] = ts
      iemob.data['year'] = ts.year
      iemob.load_and_compare(iemaccess)
      for var in mydata[sid][ts].keys():
        iemob.data[ mapping[var] ] = cleaner(mydata[sid][ts][var]) * multiplier[var]

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
