# Copyright (c) 2005-2008 Iowa State University
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
""" METAR product ingestor """

from twisted.words.protocols.jabber import client, jid, xmlstream
from twisted.internet import reactor
from twisted.python import log
log.startLogging(open('logs/metarObFe.log', 'a'))
log.FileLogObserver.timeFormat = "%Y/%m/%d %H:%M:%S %Z"

import access
import re, traceback, StringIO
from pyIEM import mesonet, ldmbridge
from twisted.enterprise import adbapi
from metar import Metar
from email.MIMEText import MIMEText
import smtplib, secret, mx.DateTime, pg
import common

dbpool = adbapi.ConnectionPool("psycopg2", database='iem', host=secret.dbhost, password=secret.dbpass)

try:
    mesosite = pg.connect('mesosite', host=secret.dbhost)
    iemaccess = pg.connect('iem', host=secret.dbhost)
    asos = pg.connect('asos', host=secret.dbhost)
except:
    mesosite = pg.connect('mesosite', host=secret.dbhost, passwd=secret.dbpass)
    iemaccess = pg.connect('iem', host=secret.dbhost, passwd=secret.dbpass)
    asos = pg.connect('asos', host=secret.dbhost, passwd=secret.dbpass)
LOC2NETWORK = {}
LOC2TZNAME = {}

def load_stations():
    rs = mesosite.query("""SELECT id, network, tzname from stations 
    where network ~* 'ASOS' or network = 'AWOS' or network = 'WTM'""").dictresult()
    for i in range(len(rs)):
        if not LOC2NETWORK.has_key( rs[i]['id'] ):
            LOC2NETWORK[ rs[i]['id'] ] = rs[i]['network']
        if rs[i]['tzname'] != None and rs[i]['tzname'] != '':
            LOC2TZNAME[ rs[i]['id'] ] = rs[i]['tzname']
    # Reload every 12 hours
    reactor.callLater(12*60*60, load_stations)

windAlerts = {}

TORNADO_RE = re.compile(r" +FC |TORNADO")
FUNNEL_RE = re.compile(r" FC |FUNNEL")
HAIL_RE = re.compile(r"GR")
EMAILS = 10

def email_error(message, product_text):
    """
    Generic something to send email error messages 
    """
    global EMAILS
    log.msg( message )
    EMAILS -= 1
    if (EMAILS < 0):
        return

    msg = MIMEText("Exception:\n%s\n\nRaw Product:\n%s" \
                 % (message, product_text))
    msg['subject'] = 'metarObFe.py Traceback'
    msg['From'] = secret.parser_user
    msg['To'] = 'akrherz@iastate.edu'
    smtp = smtplib.SMTP()
    smtp.connect()
    smtp.sendmail(msg['From'], msg['To'], msg.as_string())
    smtp.close()


class MyIEMOB( access.Ob ):
    """
    Override the iemAccessOb class with my own query engine, so that we 
    can capture errors for now!
    """

    def execQuery(self, sql, dbase, dbpool):
        """
        Execute queries with an error callback!
        """
        dbpool.runOperation( sql ).addErrback( email_error, sql)


class myProductIngestor(ldmbridge.LDMProductReceiver):

    def connectionLost(self, reason):
        print 'connectionLost', reason
        reactor.callLater(10, self.shutdown)

    def shutdown(self):
        reactor.callWhenRunning(reactor.stop)

    def processData(self, buf):
        try:
            buf = unicode(buf, errors='ignore')
            real_processor( buf.encode('ascii', 'ignore') )
        except Exception, myexp:
            email_error(myexp, buf)


def real_processor(buf):
    """
    Actually process a raw string of one or more METARs
    """
    tokens = buf.split("=")
    for metar in tokens:
        if metar.find("METAR") > -1:
            metar = metar[metar.find("METAR")+5:]
        elif metar.find("LWIS ") > -1:
            metar = metar[metar.find("LWIS ")+5:]
        elif metar.find("SPECI") > -1:
            metar = metar[metar.find("SPECI")+5:]
        elif metar.find(" NIL") > -1:
            continue
        elif len(metar.strip()) < 5:
            continue
        # We actually have something
        metar = re.sub("\015\015\012", " ", metar)
        metar = re.sub("\015", " ", metar)
        process_site(metar, metar)


def process_site(orig_metar, metar):
    """
    Actually process the raw string of a metar
    @param string original string of the metar to save in database
    @param string current working version of the string metar
    """
    # Check the length I have
    if (len(metar) < 10):
        return

    # Remove any multiple whitespace, bad chars
    metar = metar.encode('latin-1').replace('\xa0', " ").replace("\003", "")
    clean_metar = re.sub("\s+", " ", metar.strip())
    # Only process US obs for now ....
    if len(clean_metar) == 0:
        return
    try:
        mtr = Metar.Metar(clean_metar)
    except Metar.ParserError as inst:
        io = StringIO.StringIO()
        traceback.print_exc(file=io)
        log.msg( io.getvalue() )
        log.msg(clean_metar)
        errormsg = str(inst)
        if errormsg.find("Unparsed groups in body: ") == 0:
            tokens = errormsg.split(": ")
            newmetar = clean_metar.replace( tokens[1] , "")
            if newmetar != clean_metar:
                print 'NEW is', newmetar
                reactor.callLater(0, process_site, orig_metar, newmetar)
        return

    # Determine the ID, unfortunately I use 3 char ids for now :(
    if mtr.station_id is None:
        return
    iemid = mtr.station_id[-3:]
    if mtr.station_id[0] != "K":
        iemid = mtr.station_id
    if not LOC2NETWORK.has_key(iemid):
        asos.query("INSERT into unknown(id) values ('%s')" % (iemid,))
        return
    network = LOC2NETWORK[iemid]

    iem = MyIEMOB(iemid, network)
    if mtr.time is None:
        return
    gts = mx.DateTime.DateTime( mtr.time.year, mtr.time.month, 
                  mtr.time.day, mtr.time.hour, mtr.time.minute)
    # Make sure that the ob is not from the future!
    if gts > (mx.DateTime.gmt() + mx.DateTime.RelativeDateTime(hours=1)):
        log.msg("%s METAR [%s] timestamp in the future!" % (iemid, gts))
        return

    iem.data['tzname'] = LOC2TZNAME.get(iemid, 'America/Chicago')
    iem.setObTimeGMT(gts)
    iem.load_and_compare(iemaccess)
    cmetar = clean_metar.replace("'", "")

    """ Need to figure out if we have a duplicate ob, if so, check
        the length of the raw data, if greater, take the temps """
    if iem.data['old_ts'] and (iem.data['ts'] == iem.data['old_ts']) and \
       (len(iem.data['raw']) > len(cmetar)):
        print "Duplicate METAR %s OLD: %s NEW: %s" % (iemid, \
               iem.data['raw'], cmetar)
        pass
    else:
        if (mtr.temp):
            iem.data['tmpf'] = mtr.temp.value("F")
        if (mtr.dewpt):
            iem.data['dwpf'] = mtr.dewpt.value("F")
    iem.data['raw'] = orig_metar.replace("'", "")[:254]
    if mtr.wind_speed:
        iem.data['sknt'] = mtr.wind_speed.value("KT")
    if mtr.wind_gust:
        iem.data['gust'] = mtr.wind_gust.value("KT")
    if mtr.wind_dir:
        if mtr.wind_dir.value() == 'VRB':
            iem.data['drct'] = 0
        else:
            iem.data['drct'] = float(mtr.wind_dir.value())
    if mtr.vis:
        iem.data['vsby'] = mtr.vis.value("SM")
    if mtr.press:
        iem.data['alti'] = mtr.press.value("IN")
    iem.data['phour'] = 0
    if mtr.precip_1hr:
        iem.data['phour'] = mtr.precip_1hr.value("IN")
    # Do something with sky coverage
    for i in range(len(mtr.sky)):
        (c,h,b) = mtr.sky[i]
        iem.data['skyc%s' % (i+1)] = c
        if h is not None:
            iem.data['skyl%s' % (i+1)] = h.value("FT")

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
        #log.msg("PEAK GUST FOUND: %s %s %s %s" % \
        #        (mtr.station_id, v, t, clean_metar))
        if (v >= 50 and not windAlerts.has_key(key)):
            windAlerts[key] = 1
            sendWindAlert(iemid, v, d, t, clean_metar)

        d0 = {}
        d0['tzname'] = LOC2TZNAME.get(iemid, 'America/Chicago')
        d0['stationID'] = mtr.station_id[1:]
        d0['peak_gust'] = v
        d0['peak_drct'] = d
        d0['peak_ts'] = t.strftime("%Y-%m-%d %H:%M:%S")+"+00"
        d0['year'] = t.strftime("%Y")
        iem.updateDatabasePeakWind(iemaccess, d0, dbpool)


    del mtr, iem

def sendAlert(iemid, what, clean_metar):
    print "ALERTING for [%s]" % (iemid,)
    mesosite = pg.connect('mesosite', secret.dbhost)
    rs = mesosite.query("""SELECT wfo, state, name, x(geom) as lon,
           y(geom) as lat from stations 
           WHERE id = '%s' """ % (iemid,) ).dictresult()
    if len(rs) == 0:
        print "I not find WFO for sid: %s " % (iemid,)
        return
    wfo = rs[0]['wfo']
    st = rs[0]['state']
    nm = rs[0]['name']

    extra = ""
    if (clean_metar.find("$") > 0):
        extra = "(Caution: Maintenance Check Indicator)"
    jtxt = "%s: %s,%s (%s) ASOS %s reports %s\n%s http://mesonet.agron.iastate.edu/ASOS/current.phtml"\
            % (wfo, nm, st, iemid, extra, what, clean_metar )

    jabber.sendMessage(jtxt, jtxt)

    twt = "%s,%s (%s) ASOS reports %s" % (nm, st, iemid, what)
    url = "http://mesonet.agron.iastate.edu/ASOS/current.phtml?network=%s_ASOS" % (st.upper(),)
    common.tweet([wfo], twt, url, {'lat': `rs[0]['lat']`, 'long': `rs[0]['lon']`})


def sendWindAlert(iemid, v, d, t, clean_metar):
    """
    Send a wind alert please
    """
    print "ALERTING for [%s]" % (iemid,)
    mesosite = pg.connect('mesosite', secret.dbhost)
    rs = mesosite.query("""SELECT wfo, state, name, x(geom) as lon,
           y(geom) as lat from stations 
           WHERE id = '%s' """ % (iemid,) ).dictresult()
    if (len(rs) == 0):
        print "I not find WFO for sid: %s " % (iemid,)
        return
    wfo = rs[0]['wfo']
    st = rs[0]['state']
    nm = rs[0]['name']

    extra = ""
    if (clean_metar.find("$") > 0):
        extra = "(Caution: Maintenance Check Indicator)"
    jtxt = "%s: %s,%s (%s) ASOS %s reports gust of %s knots from %s @ %s\n%s"\
            % (wfo, nm, st, iemid, extra, v, mesonet.drct2dirTxt(d), \
               t.strftime("%H%MZ"), clean_metar )
    jabber.sendMessage(jtxt, jtxt)

    twt = "%s,%s (%s) ASOS reports gust of %s knots from %s @ %s" % (nm, 
     st, iemid, v, mesonet.drct2dirTxt(d), t.strftime("%H%MZ"))
    url = "http://mesonet.agron.iastate.edu/ASOS/current.phtml?network=%s_ASOS" % (st.upper(),)
    common.tweet([wfo], twt, url, {'lat': `rs[0]['lat']`, 'long': `rs[0]['lon']`})

myJid = jid.JID('%s@%s/metar_%s' % \
      (secret.iembot_ingest_user, secret.chatserver, \
       mx.DateTime.gmt().strftime("%Y%m%d%H%M%S") ) )
factory = client.basicClientFactory(myJid, secret.iembot_ingest_password)
jabber = common.JabberClient(myJid)
factory.addBootstrap('//event/stream/authd',jabber.authd)
factory.addBootstrap("//event/client/basicauth/invaliduser", jabber.debug)
factory.addBootstrap("//event/client/basicauth/authfailed", jabber.debug)
factory.addBootstrap("//event/stream/error", jabber.debug)
factory.addBootstrap(xmlstream.STREAM_END_EVENT, jabber._disconnect )
reactor.connectTCP(secret.connect_chatserver, 5222, factory)

fact = ldmbridge.LDMProductFactory( myProductIngestor() )
reactor.callLater(0, load_stations)
reactor.run()
