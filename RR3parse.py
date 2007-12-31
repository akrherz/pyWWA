
import sys, os, string, traceback
from pyIEM import shefReport, iemAccess, iemAccessOb, ldmbridge
from twisted.python import log
from twisted.internet import reactor
import smtplib, StringIO
from email.MIMEText import MIMEText


iemaccess = iemAccess.iemAccess()
log.startLogging( open('/tmp/RR3parse.log','a') )

networks = {'I4': 'IA', 'I2': 'IL', 'I3': 'IN', 'K1': 'KS', 'M4': 'MI',
            'M5': 'MN', 'M7': 'MO', 'N8': 'ND', 'N1': 'NE', 'O1': 'OH',
            'S2': 'SD', 'W3': 'WI'}


class myProductIngestor(ldmbridge.LDMProductReceiver):

  def emailErrors(self, errstr, raw):

    msg = MIMEText("%s\n\n>RAW DATA\n\n%s" % (errstr, raw.replace("\015\015\012", "\n") ) )
    msg['subject'] = 'RR3parse.py Traceback'
    msg['From'] = "ldm@mesonet.agron.iastate.edu"
    msg['To'] = "akrherz@iastate.edu"

    s = smtplib.SMTP()
    s.connect()
    s.sendmail(msg["From"], msg["To"], msg.as_string())
    s.close()


  def processData(self, buf):
    cstr = StringIO.StringIO()
    try:
      sreport = shefReport.shefReport( buf, cstr )
    except:
      traceback.print_exc(file=cstr)
      cstr.seek(0)
      errstr = cstr.read()
      self.emailErrors(errstr, buf)
      return

    #print sreport.report
    for sid in sreport.db.keys():  # Get all stations in this report
      for ts in sreport.db[sid].keys():  # Each Time
        if (ts == 'writets'):
          continue
        ooo = open("/home/ldm/scripts/rr3sites/%s" % (sid,) , 'w')
        ooo.write("\n")
        ooo.close()
        stid = sid[3:]
        if (sid == '3SE'):
          stid = 'I4'
          pass
        elif (not networks.has_key(stid)):
          continue
        iemob = iemAccessOb.iemAccessOb(sid, networks[stid]+"_COOP")
        iemob.data['ts'] = ts
        iemob.data['year'] = ts.year
        iemob.data['TA'] = -99
        iemob.data['TN'] = -99
        iemob.data['TX'] = -99
        iemob.data['PP'] = -99
        iemob.data['SF'] = -99
        iemob.data['SD'] = -99
        iemob.data['SW'] = -99
        for var in sreport.db[sid][ts].keys(): # Each var
          iemob.data[var] = string.strip( sreport.db[sid][ts][var] )
 
        if (iemob.data['TA'] != "M"):
          iemob.data['tmpf'] = iemob.data['TA']
        if (iemob.data['TX'] != "M"):
          iemob.data['max_tmpf'] = iemob.data['TX']
        if (iemob.data['TN'] != "M"):
          iemob.data['min_tmpf'] = iemob.data['TN']
        if (iemob.data.has_key('PPn')):
          iemob.data['PP'] = iemob.data['PPn']
        if (iemob.data['PP'] != "M"):
          iemob.data['pday'] = iemob.data['PP']
        if (iemob.data['SF'] != "M"):
          iemob.data['snow'] = iemob.data['SF']
        if (iemob.data['SD'] != "M"):
          iemob.data['snowd'] = iemob.data['SD']
        if (iemob.data['SW'] != "M"):
          iemob.data['snoww'] = iemob.data['SW']
        if (str(iemob.data['pday']) == "T"):
          iemob.data['pday'] = 0.0001
        if (str(iemob.data['snow']) == "T"):
          iemob.data['snow'] = 0.0001
        if (str(iemob.data['snowd']) == "T"):
          iemob.data['snowd'] = 0.0001
        iemob.updateDatabaseSummaryTemps(iemaccess.iemdb)
        iemob.updateDatabase(iemaccess.iemdb)
        print "Update :%s:" % (sid,)

#
fact = ldmbridge.LDMProductFactory( myProductIngestor() )
reactor.run()

