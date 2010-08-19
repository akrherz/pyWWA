#!/mesonet/python/bin/python
# 14 Mar 2004	Rewrite for new Reports

import re, sys, smtplib, mx.DateTime
from pyIEM import stationTable, iemdb
i = iemdb.iemdb()
from email.MIMEText import MIMEText

st = stationTable.stationTable("/mesonet/TABLES/snet.stns")

alerts = { 'KCCI': ["akrherz@iastate.edu", "wxdude@gmail.com"],
  'KIMT': ["akrherz@iastate.edu",],
  'KELO': ["akrherz@iastate.edu", "dennis_todey@sdstate.edu",
   "stormcenter@keloland.com"] }

#alerts = { 'KCCI' : ["akrherz@iastate.edu",],
#		'KELO': ["akrherz@iastate.edu",] }

bulletin = sys.stdin.read()
#blines = re.split("\n", bulletin)

tokens = re.findall(".. (.....) (....)  . ..(....)/.. (.*)", bulletin)

network = sys.argv[1]
sid = tokens[0][0]
mmdd = tokens[0][1]
hhmm = tokens[0][2]
sped = tokens[0][3]

msg = MIMEText(bulletin)

msg['Subject'] = "[%s] %s Gust %s" % (network, sped, st.sts[sid]["name"])

s = smtplib.SMTP()
s.connect()
s.sendmail("ldm@mesonet.agron.iastate.edu", alerts[network], msg.as_string() )
s.close()

if (network == 'KCCI'): # We have a special
  ts = mx.DateTime.strptime(mmdd + hhmm, "%m%d%H%M")
  ts += mx.DateTime.RelativeDateTime(year= mx.DateTime.now().year )
  mydb = i['kcci']
  emails = []
  rs = mydb.query("select w.uid, a.email from walerts w, accounts a \
    WHERE w.sid = '"+ sid +"' and w.uid = a.uid").dictresult()
  for i in range(len(rs)):
    emails.append(rs[i]['email'])

#  emails = ['akrherz@iastate.edu']

  form = {}
  form["sname"] = st.sts[sid]['name']
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

  msg['Subject'] = "[%s] %s Gust %s" % (network, sped, st.sts[sid]["name"])

  s = smtplib.SMTP()
  s.connect()
  s.sendmail("ldm@mesonet.agron.iastate.edu", emails, msg.as_string() )
  s.close()
