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
from pyIEM import shefReport, ldmbridge
from twisted.python import log
from twisted.internet import reactor
import smtplib, StringIO
from email.MIMEText import MIMEText
import secret

log.startLogging(open('/mesonet/data/logs/%s/shefParse.log' \
    % (os.getenv("USER"),), 'a'))
log.FileLogObserver.timeFormat = "%Y/%m/%d %H:%M:%S %Z"


found_vars = {}

class myProductIngestor(ldmbridge.LDMProductReceiver):

  def emailErrors(self, errstr, raw):

    msg = MIMEText("%s\n\n>RAW DATA\n\n%s" % (errstr, raw.replace("\015\015\012", "\n") ) )
    msg['subject'] = 'shefParse.py Traceback'
    msg['From'] = "ldm@mesonet.agron.iastate.edu"
    msg['To'] = "akrherz@iastate.edu"

    s = smtplib.SMTP()
    s.connect()
    s.sendmail(msg["From"], msg["To"], msg.as_string())
    s.close()
    reactor.stop()

  def processData(self, buf):
    cstr = StringIO.StringIO()
    try:
      self.real_process(buf, cstr)
    except:
      traceback.print_exc(file=cstr)
      cstr.seek(0)
      errstr = cstr.read()
      self.emailErrors(errstr, buf)
      return

  def real_process(self, buf, cstr):
    sreport = shefReport.shefReport(buf, cstr )

    for sid in sreport.db.keys():  # Get all stations in this report

      for ts in sreport.db[sid].keys():  # Each Time
        if (ts == 'writets'):
          continue
        for var in sreport.db[sid][ts].keys(): # Each var
          found_vars[ var ] = 1

def writeVars():
  print found_vars.keys()
#
fact = ldmbridge.LDMProductFactory( myProductIngestor() )
reactor.callLater(60, writeVars)
reactor.run()
