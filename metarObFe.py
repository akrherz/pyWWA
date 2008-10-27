from twisted.words.protocols.jabber import client, jid, xmlstream
from twisted.words.xish import domish
from twisted.internet import reactor
from twisted.python import log
import os
log.startLogging(open('logs/metarObFe.log', 'a'))
log.FileLogObserver.timeFormat = "%Y/%m/%d %H:%M:%S %Z"


import  sys, re, string, traceback, StringIO
from pyIEM import iemAccess, iemAccessOb, mesonet, ldmbridge, iemdb
from twisted.enterprise import adbapi
from metar import Metar
from email.MIMEText import MIMEText
import smtplib, secret, mx.DateTime
from common import *


dbpool = adbapi.ConnectionPool("psycopg2", database='iem', host=secret.dbhost)

iemaccess = iemAccess.iemAccess()

windAlerts = {}

TORNADO_RE = re.compile(r" +FC |TORNADO")
FUNNEL_RE = re.compile(r" FC |FUNNEL")
HAIL_RE = re.compile(r"GR")

class myProductIngestor(ldmbridge.LDMProductReceiver):

    def connectionLost(self, reason):
        print 'connectionLost', reason
        reactor.callLater(5, self.shutdown)

    def shutdown(self):
        reactor.callWhenRunning(reactor.stop)

    def processData(self, buf):
        try:
            real_processor(buf)
        except:
            io = StringIO.StringIO()
            traceback.print_exc(file=io)
            log.msg( io.getvalue() )
            msg = MIMEText("%s\n\n>RAW DATA\n\n%s"%(io.getvalue(),buf.replace("\015\015\012", "\n") ))
            msg['subject'] = 'metarObFe.py Traceback'
            msg['From'] = secret.parser_user
            msg['To'] = "akrherz@iastate.edu"

            s = smtplib.SMTP()
            s.connect()
            s.sendmail(msg["From"], msg["To"], msg.as_string())
            s.close()



def real_processor(buf):
    tokens = re.split("METAR", buf)
    if (len(tokens) == 1):
        tokens = re.split("SPECI", buf)
        if (len(tokens) == 1):
            log.msg("Nothing found: %s" % (buf) )
            return
    dataSect = tokens[1]
    dataSect = re.sub("\015\015\012", " ", dataSect)
    dataSect = re.sub("\015", " ", dataSect)
    metars = re.split("=", dataSect)

    for metar in metars:
        if (len(metar) < 10):
            continue
        clean_metar = re.sub("\s+", " ", metar.strip())
        if (len(clean_metar) == 0 or clean_metar[0] not in ("K","X")):
            continue
        try:
          mtr = Metar.Metar(clean_metar)
        except Metar.ParserError:
          io = StringIO.StringIO()
          traceback.print_exc(file=io)
          log.msg( io.getvalue() )
          log.msg(clean_metar)
          continue

        iemid = mtr.station_id[-3:]
        if (mtr.station_id[0] == "X"): # West Texas Mesonet
            iemid = mtr.station_id
        iem = iemAccessOb.iemAccessOb(iemid)
        gts = mx.DateTime.DateTime( mtr.time.year, mtr.time.month, mtr.time.day, mtr.time.hour, mtr.time.minute)

        iem.setObTimeGMT(gts)
        iem.load_and_compare(iemaccess.iemdb)
        if (mtr.temp):
            iem.data['tmpf'] = mtr.temp.value("F")
        if (mtr.dewpt):
            iem.data['dwpf'] = mtr.dewpt.value("F")
        if mtr.wind_speed:
            iem.data['sknt'] = mtr.wind_speed.value("KT")
        if mtr.wind_gust:
            iem.data['gust'] = mtr.wind_gust.value("KT")
        if mtr.wind_dir:
            iem.set_drct(mtr.wind_dir.value())
        if mtr.vis:
            iem.data['vsby'] = mtr.vis.value("SM")
        if mtr.press:
            iem.data['alti'] = mtr.press.value("IN")
        iem.data['phour'] = 0
        if mtr.precip_1hr:
            iem.data['phour'] = mtr.precip_1hr.value("IN")
        iem.data['raw'] = clean_metar
        iem.updateDatabase(None, dbpool)

        # Search for tornado
        if (len(TORNADO_RE.findall(clean_metar)) > 0):
            sendAlert(iemid, "Tornado", clean_metar)
        elif (len(FUNNEL_RE.findall(clean_metar)) > 0):
            sendAlert(iemid, "Funnel", clean_metar)
        else:
            for weatheri in mtr.weather:
                for x in weatheri:
                    if x is not None and "GR" in x:
                        sendAlert(iemid, "Hail", clean_metar)
           

        # Search for Peak wind gust info....
        if (mtr.wind_gust or mtr.wind_speed_peak):
            d = 0
            v = 0
            if (mtr.wind_gust):
                v = mtr.wind_gust.value("KT")
                if (mtr.wind_dir):
                    d = mtr.wind_dir.value()
                t = mtr.time
            if (mtr.wind_speed_peak):
                v1 = mtr.wind_speed_peak.value("KT")
                d1 = mtr.wind_dir_peak.value()
                t1 = mtr.peak_wind_time
                if (v1 > v):
                    v = v1
                    d = d1
                    t = t1

            # We store a key for this event
            key = "%s;%s;%s" % (mtr.station_id, v, t)
            log.msg("PEAK GUST FOUND: %s %s %s %s" % (mtr.station_id, v, t, clean_metar))
            if (v >= 50 and not windAlerts.has_key(key)):
                windAlerts[key] = 1
                sendWindAlert(iemid, v, d, t, clean_metar)


            d0 = {}
            d0['stationID'] = mtr.station_id
            d0['peak_gust'] = v
            d0['peak_ts'] = t.strftime("%Y-%m-%d %H:%M:%S")
            d0['year'] = t.strftime("%Y")
            iem.updateDatabasePeakWind(iemaccess.iemdb, d0, dbpool)


        del mtr, iem
    del tokens, metars, dataSect

def sendAlert(iemid, what, clean_metar):
    print "ALERTING"
    i = iemdb.iemdb(dhost=secret.dbhost)
    mesosite = i['mesosite']
    rs = mesosite.query("SELECT wfo, state, name from stations \
           WHERE id = '%s' " % (iemid,) ).dictresult()
    if (len(rs) == 0):
      print "I not find WFO for sid: %s " % (iemid,)
      rs = [{'wfo': 'XXX', 'state': 'XX', 'name': 'XXXX'},]
    wfo = rs[0]['wfo']
    st = rs[0]['state']
    nm = rs[0]['name']

    extra = ""
    if (clean_metar.find("$") > 0):
      extra = "(Caution: Maintenance Check Indicator)"
    jabberTxt = "%s: %s,%s (%s) ASOS %s reports %s\n%s"\
            % (wfo, nm, st, iemid, extra, what, clean_metar )

    jabber.sendMessage(jabberTxt)


def sendWindAlert(iemid, v, d, t, clean_metar):
    i = iemdb.iemdb(dhost=secret.dbhost)
    mesosite = i['mesosite']
    rs = mesosite.query("SELECT wfo, state, name from stations \
           WHERE id = '%s' " % (iemid,) ).dictresult()
    if (len(rs) == 0):
      print "I not find WFO for sid: %s " % (iemid,)
      return
    wfo = rs[0]['wfo']
    st = rs[0]['state']
    nm = rs[0]['name']

    extra = ""
    if (clean_metar.find("$") > 0):
      extra = "(Caution: Maintenance Check Indicator)"
    jabberTxt = "%s: %s,%s (%s) ASOS %s reports gust of %s knots from %s @ %s\n%s"\
            % (wfo, nm, st, iemid, extra, v, mesonet.drct2dirTxt(d), \
               t.strftime("%H%MZ"), clean_metar )

    jabber.sendMessage(jabberTxt)

myJid = jid.JID('%s@%s/metar_%s' % \
      (secret.iembot_ingest_user, secret.chatserver, \
       mx.DateTime.gmt().strftime("%Y%m%d%H%M%S") ) )
factory = client.basicClientFactory(myJid, secret.iembot_ingest_password)
jabber = JabberClient(myJid)
factory.addBootstrap('//event/stream/authd',jabber.authd)
factory.addBootstrap("//event/client/basicauth/invaliduser", jabber.debug)
factory.addBootstrap("//event/client/basicauth/authfailed", jabber.debug)
factory.addBootstrap("//event/stream/error", jabber.debug)
factory.addBootstrap(xmlstream.STREAM_END_EVENT, jabber._disconnect )
reactor.connectTCP(secret.connect_chatserver,5222,factory)


fact = ldmbridge.LDMProductFactory( myProductIngestor() )



reactor.run()

