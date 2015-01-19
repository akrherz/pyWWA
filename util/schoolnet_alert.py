"""
Called when there is a schoolnet alert
"""

import re
import sys
import smtplib
import datetime
from email.MIMEText import MIMEText
from pyiem.network import Table as NetworkTale

nt = NetworkTable(('KCCI','KELO','KIMT'))

alerts = { 'KCCI': ["akrherz@iastate.edu", "wxdude@gmail.com"],
  'KIMT': ["akrherz@iastate.edu",],
  'KELO': ["akrherz@iastate.edu", "dennis_todey@sdstate.edu",
   "stormcenter@keloland.com"] }

bulletin = sys.stdin.read()

tokens = re.findall(".. (.....) (....)  . ..(....)/.. (.*)", bulletin)

network = sys.argv[1]
sid = tokens[0][0]
mmdd = tokens[0][1]
hhmm = tokens[0][2]
sped = tokens[0][3]

msg = MIMEText(bulletin)

msg['Subject'] = "[%s] %s Gust %s" % (network, sped, nt.sts[sid]["name"])

s = smtplib.SMTP()
s.connect()
s.sendmail("ldm@mesonet.agron.iastate.edu", alerts[network], msg.as_string() )
s.close()

if network != 'KCCI': # We have a special
    sys.exit()

ts = datetime.datetime.strptime("%s%s%s" % (datetime.datetime.now().year,
                                            mmdd, hhmm), "%m%d%H%M")
import psycopg2
KCCI = psycopg2.connect(database='kcci', host='iemdb')
kcursor = KCCI.cursor()
emails = []
kcursor.execute("""select w.uid, a.email from walerts w, accounts a 
    WHERE w.sid = %s and w.uid = a.uid""", (sid,))
for row in kcursor:
    emails.append(row[1])


form = {}
form["sname"] = nt.sts[sid]['name']
form["gust"] = sped
form["obts"] = ts.strftime("%I:%M %P -- %d %B %Y")
form["sid"] = sid
form["year"] = ts.strftime("%Y")
form["month"] = ts.strftime("%m")
form["day"] = ts.strftime("%d")
form["bulletin"] = bulletin

mformat = """
====================================================================
KCCI SchoolNet8 Wind Gust Alert
-------------------------------

  Time:  %(obts)s
  Site:  %(sname)s
  Gust:  %(gust)s MPH


%(sname)s  Links:
 [Current Conditions & Live Super Doppler]
   http://www.schoolnet8.com/site.phtml?station=%(sid)s

 [1 Minute Data Trace]
   http://www.schoolnet8.com/plotting/1trace_fe.phtml?station=%(sid)s&year=%(year)s&month=%(month)s&day=%(day)s

Raw Report:%(bulletin)s

====================================================================
 * This email has been sent you based on your subscription to the
 KCCI SchoolNet8 Email Alerts.  You can modify your subscription at
 this URL.
   http://kcci.mesonet.agron.iastate.edu/tool/walerts.phtml

 * Questions about this service can be sent to 
 Daryl Herzmann akrherz@iastate.edu

====================================================================
"""

mstring = mformat % form

msg = MIMEText(mstring)
msg['Subject'] = "[%s] %s Gust %s" % (network, sped, nt.sts[sid]["name"])

s = smtplib.SMTP()
s.connect()
s.sendmail("ldm@mesonet.agron.iastate.edu", emails, msg.as_string() )
s.close()
