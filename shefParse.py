
import sys, sys, os
import traceback
from pyIEM import shefReport, ldmbridge
from twisted.python import log
from twisted.internet import reactor
from settings import *
import smtplib, StringIO
from email.MIMEText import MIMEText


log.startLogging( open('/tmp/shefParse.log','a') )

found_vars = {}

class myProductIngestor(ldmbridge.LDMProductReceiver):

  def emailErrors(self, errstr, raw):

    msg = MIMEText("%s\n\n>RAW DATA\n\n%s" % (errstr, raw.replace("\015\015\012", "\n") ) )
    msg['subject'] = 'shefParse.py Traceback'
    msg['From'] = errors_from
    msg['To'] = errors_to[0]

    s = smtplib.SMTP()
    s.connect()
    s.sendmail(errors_from, errors_to, msg.as_string())
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
