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
from twisted.python import log, logfile
log.FileLogObserver.timeFormat = "%Y/%m/%d %H:%M:%S %Z"
log.startLogging(logfile.DailyLogFile('metarObFe.log', 'logs/'))


import access
import re, traceback, StringIO
from pyIEM import mesonet, ldmbridge
from twisted.enterprise import adbapi
from metar import Metar
import secret, mx.DateTime
import common

IEMDB = adbapi.ConnectionPool("twistedpg", database='iem', host=secret.dbhost, 
                              password=secret.dbpass, cp_reconnect=True)
ASOSDB = adbapi.ConnectionPool("twistedpg", database='asos', host=secret.dbhost, 
                               password=secret.dbpass, cp_reconnect=True)

    
LOC2NETWORK = {}

def load_stations(txn, ingest):
    txn.execute("""SELECT id, network from stations 
    where network ~* 'ASOS' or network = 'AWOS' or network = 'WTM'""")
    news = 0
    for row in txn:
        if not LOC2NETWORK.has_key( row['id'] ):
            news += 1
            LOC2NETWORK[ row['id'] ] = row['network']
    log.msg("Loaded %s new stations" % (news,))
    ingest.stations_loaded = True
    # Reload every 12 hours
    reactor.callLater(12*60*60, IEMDB.runInteraction, load_stations, ingest)

windAlerts = {}

TORNADO_RE = re.compile(r" +FC |TORNADO")
FUNNEL_RE = re.compile(r" FC |FUNNEL")
HAIL_RE = re.compile(r"GR")


class myProductIngestor(ldmbridge.LDMProductReceiver):
    stations_loaded = False

    def connectionLost(self, reason):
        print 'connectionLost', reason
        reactor.callLater(30, self.shutdown)

    def shutdown(self):
        reactor.callWhenRunning(reactor.stop)

    def processData(self, buf):

        buf = unicode(buf, errors='ignore')
        if self.stations_loaded:
            reactor.callLater(0, real_processor, buf.encode('ascii', 'ignore') )
        else:
            log.msg("Stations not loaded, delaying!")
            reactor.callLater(20, real_processor, buf.encode('ascii', 'ignore') )

def sanitize(metar):
    """
    Preprocess our metar into something we can deal with :/
    @param metar string
    @return metar string
    """
    metar = re.sub("\015\015\012", " ", metar)
    metar = re.sub("\015", " ", metar)
    # Remove any multiple whitespace, bad chars
    metar = metar.encode('latin-1').replace('\xa0', " ").replace("\003", "").replace("COR ", "")
    metar = re.sub("\s+", " ", metar.strip())
    # Look to see that our METAR starts with A-Z
    if re.match("^[0-9]", metar):
        log.msg("Found METAR starting with number, gleaning: %s" % (metar,))
        tokens = metar.split()
        metar = " ".join(tokens[1:])
    return metar

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
        metar = sanitize( metar )
        process_site(metar, metar)


def process_site(orig_metar, clean_metar):
    """
    Actually process the raw string of a metar
    @param string original string of the metar to save in database
    @param string current working version of the string metar
    """
    # Check the length I have
    if len(clean_metar) < 10:
        return
    try:
        mtr = Metar.Metar(clean_metar)
    except Metar.ParserError as inst:
        io = StringIO.StringIO()
        traceback.print_exc(file=io)
        errormsg = str(inst)
        if errormsg.find("Unparsed groups in body: ") == 0:
            tokens = errormsg.split(": ")
            newmetar = clean_metar.replace( tokens[1] , "")
            if newmetar != clean_metar:
                reactor.callLater(0, process_site, orig_metar, newmetar)
        else:
            log.msg( io.getvalue() )
            log.msg(clean_metar)
        return

    # Determine the ID, unfortunately I use 3 char ids for now :(
    if mtr.station_id is None:
        log.msg("METAR station_id is None: %s" % (orig_metar,))
        return
    iemid = mtr.station_id[-3:]
    if mtr.station_id[0] != "K":
        iemid = mtr.station_id
    if not LOC2NETWORK.has_key(iemid):
        log.msg("Unknown ID: %s: %s" % (iemid, orig_metar))
        deffer = ASOSDB.runOperation("INSERT into unknown(id) values (%s)", (iemid,))
        deffer.addErrback(common.email_error, iemid)
        return
    network = LOC2NETWORK[iemid]

    iem = access.Ob(iemid, network)
    if mtr.time is None:
        log.msg("%s METAR has none-time: %s" % (iemid, orig_metar))
        return
    gts = mx.DateTime.DateTime( mtr.time.year, mtr.time.month, 
                  mtr.time.day, mtr.time.hour, mtr.time.minute)
    # Make sure that the ob is not from the future!
    if gts > (mx.DateTime.gmt() + mx.DateTime.RelativeDateTime(hours=1)):
        log.msg("%s METAR [%s] timestamp in the future!" % (iemid, gts))
        return
    iem.setObTimeGMT(gts)
    deffer = IEMDB.runInteraction(save_data, iemid, iem, mtr, clean_metar, orig_metar)
    deffer.addErrback(common.email_error, clean_metar)
    #deffer.addCallback(got_results, tp, sid, network)
    
def save_data(txn, iemid, iem, mtr, clean_metar, orig_metar):
    iem.txn = txn
    if not iem.load_and_compare():
        log.msg("load and compare failed: %s %s" % (iemid,clean_metar))
        deffer = ASOSDB.runOperation("INSERT into unknown(id) values (%s)", (iemid,))
        deffer.addErrback(common.email_error, iemid)
        return
    
    cmetar = clean_metar.replace("'", "")

    """ Need to figure out if we have a duplicate ob, if so, check
        the length of the raw data, if greater, take the temps """
    if iem.data['old_ts'] and (iem.data['ts'] == iem.data['old_ts']) and (len(iem.data['raw']) > len(cmetar)):
        pass
    else:
        if (mtr.temp):
            iem.data['tmpf'] = mtr.temp.value("F")
        if (mtr.dewpt):
            iem.data['dwpf'] = mtr.dewpt.value("F")
    # Database only allows len 254
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
            
    if not mtr.wind_speed_peak:
        old_max_wind = max([iem.data.get('max_sknt', 0),
                    iem.data.get('max_gust', 0)])
        new_max_wind = max([iem.data.get('sknt', 0),
                    iem.data.get('gust', 0)])
        if new_max_wind > old_max_wind:
            #print 'Setting max_drct manually: %s' % (clean_metar,)
            iem.data['max_drct'] = iem.data.get('drct',0)

    if mtr.max_temp_6hr:
        iem.data['max_tmpf_6hr'] = mtr.max_temp_6hr.value("F")
    if mtr.min_temp_6hr:
        iem.data['min_tmpf_6hr'] = mtr.min_temp_6hr.value("F")
    if mtr.max_temp_24hr:
        iem.data['max_tmpf_24hr'] = mtr.max_temp_24hr.value("F")
    if mtr.min_temp_24hr: 
        iem.data['min_tmpf_6hr'] = mtr.min_temp_24hr.value("F")
    if mtr.precip_3hr:
        iem.data['p03i'] = mtr.precip_3hr.value("IN")
    if mtr.precip_6hr:
        iem.data['p06i'] = mtr.precip_6hr.value("IN")
    if mtr.precip_24hr:
        iem.data['p24i'] = mtr.precip_24hr.value("IN")

    if mtr.vis:
        iem.data['vsby'] = mtr.vis.value("SM")
    if mtr.press:
        iem.data['alti'] = mtr.press.value("IN")
    if mtr.press_sea_level:
        iem.data['pres'] = mtr.press_sea_level.value("MB")
    iem.data['phour'] = 0
    if mtr.precip_1hr:
        iem.data['phour'] = mtr.precip_1hr.value("IN")
    # Do something with sky coverage
    for i in range(len(mtr.sky)):
        (c,h,b) = mtr.sky[i]
        iem.data['skyc%s' % (i+1)] = c
        if h is not None:
            iem.data['skyl%s' % (i+1)] = h.value("FT")

    iem.updateDatabase()

    # Search for tornado
    if (len(TORNADO_RE.findall(clean_metar)) > 0):
        sendAlert(txn, iemid, "Tornado", clean_metar)
    elif (len(FUNNEL_RE.findall(clean_metar)) > 0):
        sendAlert(txn, iemid, "Funnel", clean_metar)
    else:
        for weatheri in mtr.weather:
            for x in weatheri:
                if x is not None and "GR" in x:
                    sendAlert(txn, iemid, "Hail", clean_metar)
           

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
            sendWindAlert(txn, iemid, v, d, t, clean_metar)

        d0 = {}
        d0['stationID'] = mtr.station_id[1:]
        d0['network'] = iem.data.get('network')
        d0['peak_gust'] = v
        d0['peak_drct'] = d
        d0['peak_ts'] = t.strftime("%Y-%m-%d %H:%M:%S")+"+00"
        d0['year'] = t.strftime("%Y")
        iem.updateDatabasePeakWind(None, d0)


    del mtr, iem

def sendAlert(txn, iemid, what, clean_metar):
    print "ALERTING for [%s]" % (iemid,)
    txn.execute("""SELECT wfo, state, name, x(geom) as lon,
           y(geom) as lat, network from stations 
           WHERE id = '%s' """ % (iemid,) )
    if txn.rowcount == 0:
        print "I not find WFO for sid: %s " % (iemid,)
        return
    row = txn.fetchone()
    wfo = row['wfo']
    if wfo is None or wfo == '':
        log.msg("Unknown WFO for id: %s, skipping alert" % (iemid,))
        return
    st = row['state']
    nm = row['name']
    network = row['network']

    extra = ""
    if (clean_metar.find("$") > 0):
        extra = "(Caution: Maintenance Check Indicator)"
    url = "http://mesonet.agron.iastate.edu/ASOS/current.phtml?network=%s" % (network,)
    jtxt = "%s: %s,%s (%s) ASOS %s reports %s\n%s %s" % (wfo, nm, st, iemid, extra, what, clean_metar, url )

    jabber.sendMessage(jtxt, jtxt)

    twt = "%s,%s (%s) ASOS reports %s" % (nm, st, iemid, what)
    common.tweet([wfo], twt, url, {'lat': str(row['lat']), 'long': str(row['lon'])})


def sendWindAlert(txn, iemid, v, d, t, clean_metar):
    """
    Send a wind alert please
    """
    print "ALERTING for [%s]" % (iemid,)
    txn.execute("""SELECT wfo, state, name, x(geom) as lon,
           y(geom) as lat, network from stations 
           WHERE id = '%s' """ % (iemid,) )
    if txn.rowcount == 0:
        print "I not find WFO for sid: %s " % (iemid,)
        return
    row = txn.fetchone()
    wfo = row['wfo']
    if wfo is None or wfo == '':
        log.msg("Unknown WFO for id: %s, skipping WindAlert" % (iemid,))
        return
    st = row['state']
    nm = row['name']
    network = row['network']

    extra = ""
    if (clean_metar.find("$") > 0):
        extra = "(Caution: Maintenance Check Indicator)"
    url = "http://mesonet.agron.iastate.edu/ASOS/current.phtml?network=%s" % (network,)
    jtxt = "%s: %s,%s (%s) ASOS %s reports gust of %s knots from %s @ %s\n%s"\
            % (wfo, nm, st, iemid, extra, v, mesonet.drct2dirTxt(d), \
               t.strftime("%H%MZ"), clean_metar )
    jabber.sendMessage(jtxt, jtxt)

    twt = "%s,%s (%s) ASOS reports gust of %s knots from %s @ %s" % (nm, 
     st, iemid, v, mesonet.drct2dirTxt(d), t.strftime("%H%MZ"))
    common.tweet([wfo], twt, url, {'lat': str(row['lat']), 'long': str(row['lon'])})

myJid = jid.JID('%s@%s/metar_%s' % (secret.iembot_ingest_user, secret.chatserver, 
       mx.DateTime.gmt().strftime("%Y%m%d%H%M%S") ) )
factory = client.basicClientFactory(myJid, secret.iembot_ingest_password)
jabber = common.JabberClient(myJid)
factory.addBootstrap('//event/stream/authd',jabber.authd)
factory.addBootstrap("//event/client/basicauth/invaliduser", jabber.debug)
factory.addBootstrap("//event/client/basicauth/authfailed", jabber.debug)
factory.addBootstrap("//event/stream/error", jabber.debug)
factory.addBootstrap(xmlstream.STREAM_END_EVENT, jabber._disconnect )
reactor.connectTCP(secret.connect_chatserver, 5222, factory)

ingest = myProductIngestor()
fact = ldmbridge.LDMProductFactory( ingest )
IEMDB.runInteraction(load_stations, ingest)
reactor.run()
