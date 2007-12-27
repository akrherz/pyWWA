#!/usr/bin/env python

import  sys, re, string, os, traceback, StringIO
from pyIEM import iemAccess, iemAccessOb, mesonet, ldmbridge
from twisted.python import log
from twisted.internet import reactor
from twisted.enterprise import adbapi
from metar import Metar
from email.MIMEText import MIMEText
import smtplib, secret, mx.DateTime


dbpool = adbapi.ConnectionPool("psycopg2", database='iem', host=secret.dbhost)

iemaccess = iemAccess.iemAccess()
log.startLogging( open('/tmp/metarParse.log','a') )

regression = 0
if (len(sys.argv) > 1): # We are in regression mode
    regression = 1

class myProductIngestor(ldmbridge.LDMProductReceiver):

    def connectionLost(self, reason):
        print 'connectionLost', reason
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
            msg['From'] = "ldm@mesonet.agron.iastate.edu"
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
        if (clean_metar[0] not in ("K","X")):
            continue
        try:
          mtr = Metar.Metar(clean_metar)
        except Metar.ParserError:
          o = open('/tmp/metarParseFail.txt', 'a')
          o.write("\n\n\n--------------------------------------\n")
          traceback.print_exc(file=o)
          o.write(clean_metar)
          o.close()
          continue
        if (regression):
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
            iem.data['phour'] = mtr.precip_1hr.value("CM") * 10.0
        iem.data['raw'] = clean_metar
        iem.updateDatabase(None, dbpool)

        if mtr.wind_gust:
            log.msg("%s-%s-%s-%s\n" \
           % ('PGUST:', mtr.station_id, mtr.wind_gust.value("KT"), mtr.time))
            d = {}
            d['stationID'] = mtr.station_id
            d['peak_gust'] = mtr.wind_gust.value("KT")
            d['peak_ts'] = mtr.time.strftime("%Y-%m-%d %H:%M:%S")
            iem.updateDatabasePeakWind(iemaccess.iemdb, d, dbpool)

        del mtr, iem
    del tokens, metars, dataSect

fact = ldmbridge.LDMProductFactory( myProductIngestor() )

reactor.run()

