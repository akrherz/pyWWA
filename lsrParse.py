# Ingest LSRs!!!
# Daryl Herzmann 9 May 2005

# CREATE TABLE lsrs ( valid timestamp with time zone, type char(1), magnitude real, city varchar(32), county varchar(32), state char(2), source varchar(32), remark text, wfo char(3));
# SELECT AddGeometryColumn('postgis', 'lsrs', 'geom', 4326, 'POINT', 2);

# Standard python imports
import sys, re, traceback, StringIO, logging, pickle, os
from email.MIMEText import MIMEText
import smtplib

# Third party python stuff
import mx.DateTime, pg
from twisted.words.protocols.jabber import client, jid, xmlstream
from twisted.words.xish import domish
from twisted.internet import reactor

# IEM python Stuff
from common import *
import secret
from pyIEM import nws_text,  ldmbridge

postgisdb = pg.connect(host=secret.dbhost,user=secret.dbuser,dbname=secret.dbname)



errors = StringIO.StringIO()

logging.basicConfig(filename='/mesonet/data/logs/%s/lsrParse.log' % (os.getenv("USER"), ), filemode='a')
logger=logging.getLogger()
logger.addHandler(logging.StreamHandler())
logger.setLevel(logging.INFO)

hailsize = {
 0.25 : "pea",
 0.50 : "marble",
 0.75 : "penny",
 0.88 : "nickel",
 1.00 : "quarter",
 1.25 : "half dollar",
 1.50 : "ping pong ball",
 1.75 : "golf ball",
 2.00 : "egg",
 2.50 : "tennis ball",
 2.75 : "baseball",
 3.00 : "teacup",
 4.00 : "grapefruit",
 4.50 : "softball"}

events = {
 'BLOWING SNOW': 'a',
 'DRIFTING SNOW': 'a',
 'HIGH SUST WINDS': 'A',
 'DOWNBURST': 'B',
 'FUNNEL CLOUD': 'C',
 'FUNNEL': 'C',
 'TSTM WND DMG': 'D',
 'TREES DOWN': 'D',
 'TSTM WIND DMG': 'D',
 'FLOOD': 'E',
 'FLOODING': 'E',
 'FLASH FLOOD' : 'F',
 'MAJ FLASH FLD': 'F',
 'TSTM WND GST': 'G',
 'TSTM WIND': 'G',
 'TSTM WIND GST': 'G',
 'HAIL': 'H',
 'MARINE HAIL': 'H',
 'EXCESSIVE HEAT': 'I',
 'DENSE FOG': 'J',
 'LIGHTNING STRIKE': 'K',
 'LIGHTNING': 'L',
 'MARINE TSTM WND': 'M',
 'MARINE TSTM WIND': 'M',
 'NON-TSTM WND GST': 'N',
 'NON TSTM WND GST': 'N',
 'NON-TSTM WND DMG': 'O',
 'NON TSTM WND DMG': 'O',
 'NON-TSTM DMG GST': 'O',
 'NON TSTM DMG GST': 'O',
 'NON-TSTM DMG': 'O',
 'NON-TSTM WND': 'O',
 'HIGH WINDS': 'O',
 'WND DAMAGE': 'O',
 'RIP CURRENTS': 'P',
 'RIP CURRENT': 'P',
 'HIGH SURF': 'P',
 'TROPICAL STORM': 'Q',
 'HEAVY RAIN': 'R',
 'SNOW': 'S',
 'SLEET': 's',
 'MODERATE SLEET': 's',
 'HEAVY SLEET': 's',
 'HEAVY SNOW': 'S',
 'TORNADO' : 'T',
 'WILDFIRE' : 'U',
 'FIRE' : 'U',
 'AVALANCHE': 'V',
 'WALL CLOUD': 'X',
 'WATER SPOUT': 'W',
 'WATERSPOUT': 'W',
 'BLIZZARD' : 'Z',
 'HURRICANE': '0',
 'STORM SURGE': '1',
 'DUST STORM': '2',
 'SPRINKLES - FEW': '3',
 'HIGH ASTR TIDES': '4',
 'LOW ASTR TIDES': '4',
 'FREEZING RAIN': '5',
 'FREEZING DRIZZLE': '5',
 'ICE STORM': '5',
 'ICING ON ROADS': '5',
 'EXTREME COLD': '6',
 'FREEZE': '6',
 'EXTR WIND CHILL': '7',
 'WILDFIRE': '8',
}

#for k in events.keys():
#  print "\"%s\" => Array(\"name\" => \"%s\")," % (events[k], k)
#sys.exit()

offsets = {
 'EDT': 4,
 'CDT': 5, 'EST': 5,
 'MDT': 6, 'CST': 6,
 'PDT': 7, 'MST': 7,
 'ADT': 8, 'PST': 8,
 'HDT': 9, 'AST': 9,
           'HST':10
}

# Cheap datastore for LSRs to avoid Dups!
lsrdb = {}
try:
  lsrdb = pickle.load( open('lsrdb.p') )
except:
  pass

def cleandb():
    thres = mx.DateTime.gmt() - mx.DateTime.RelativeDateTime(hours=48)
    init_size = len(lsrdb.keys())
    for key in lsrdb.keys():
        if (lsrdb[key] < thres):
            del lsrdb[key]

    fin_size = len(lsrdb.keys())
    logger.info("Called cleandb()  init_size: %s  final_size: %s" % (init_size, fin_size) )
    pickle.dump(lsrdb, open('lsrdb.p','w'))

    # Call Again in 30 minutes
    reactor.callLater(60*30, cleandb) 


# LDM Ingestor
class myProductIngestor(ldmbridge.LDMProductReceiver):

    def processData(self, buf):
        try:
            real_processor(buf)
        except:
            io = StringIO.StringIO()
            traceback.print_exc(file=io)
            logger.error( io.getvalue() )
            msg = MIMEText("%s\n\n>RAW DATA\n\n%s"%(io.getvalue(),buf.replace("\015\015\012", "\n") ))
            msg['subject'] = 'lsrParse.py Traceback'
            msg['From'] = "ldm@mesonet.agron.iastate.edu"
            msg['To'] = "akrherz@iastate.edu"

            s = smtplib.SMTP()
            s.connect()
            s.sendmail(msg["From"], msg["To"], msg.as_string())
            s.close()

    def connectionLost(self,reason):
        logger.info("LDM Closed PIPE")


def real_processor(raw):
    raw = raw.replace("\015\015\012", "\n")
    nws = nws_text.nws_text(raw)
    # Need to find wfo
    tokens = re.findall("LSR([A-Z][A-Z][A-Z,0-9])\n", raw)
    wfo = tokens[0]

    # Old hack?
    #if (len(re.findall("NY[0-9]",wfo)) > 0):  
    #    return

    tsoff = mx.DateTime.RelativeDateTime(hours= offsets[nws.z])

    #isSummary = 0
    #tokens = re.findall("SUMMARY", raw)
    #if (len(tokens) > 0):
    #    isSummary = 1

    #if (nws.issued < (mx.DateTime.gmt() - mx.DateTime.RelativeDateTime(hours=6))):
    #    logger.info("OLD! %s" % (nws.issued,) )
    #    isSummary = 1

    goodies = "\n".join( nws.sections[3:] )
    data = re.split("&&", goodies)
    lines = re.split("\n", data[0])

    _state = 0
    i = 0
    sentMessages = 0
    while (i < len(lines)):
        # Line must start with a number?
        if (len(lines[i]) < 40 or (re.match("[0-9]", lines[i][0]) == None)):
            i += 1
            continue
        # We can safely eat this line
        #0914 PM     HAIL             SHAW                    33.60N 90.77W
        tq = re.split(" ", lines[i])
        hh = tq[0][:-2]
        mm = tq[0][-2:]
        am = tq[1]
        type = (lines[i][12:29]).strip().upper()
        city = (lines[i][29:53]).strip().title()
        lalo = lines[i][53:]
        tokens = lalo.strip().split()
        lat = tokens[0][:-1]
        lon = tokens[1][:-1]

        i += 1
        # And safely eat the next line
        #04/29/2005  1.00 INCH        BOLIVAR            MS   EMERGENCY MNGR
        dstr = "%s:%s %s %s" % (hh,mm,am, lines[i][:10])
        ts = mx.DateTime.strptime(dstr, "%I:%M %p %m/%d/%Y")
        magf = (lines[i][12:29]).strip()
        mag = re.sub("(ACRE|INCHES|INCH|MPH|U|FT|F|E|M|TRACE)", "", magf)
        if (mag == ""): mag = 0
        cnty = (lines[i][29:48]).strip().title()
        st = lines[i][48:50]
        source = (lines[i][53:]).strip().lower()

        # Now we search
        searching = 1
        remark = ""
        while (searching):
            i += 1
            if (len(lines) == i):
                break
            #print i, lines[i], len(lines[i])
            if (len(lines[i]) == 0 or lines[i][0] == " " or lines[i][0] == "\n"):
                remark += lines[i]
            else:
                break

        remark = remark.lower().strip()
        remark = re.sub("[\s]{2,}", " ", remark)
        remark = remark.replace("&", "&amp;").replace(">", "&gt;").replace("<","&lt;")
        gmt_ts = ts + tsoff
        dbtype = events[type]
        mag_long = ""
        if (type == "HAIL" and hailsize.has_key(float(mag))):
            mag_long = "of %s size (%s) " % (hailsize[float(mag)], magf)
        elif (mag != 0):
            mag_long = "of %s " % (magf,)
        time_fmt = "%I:%M %p"
        if (ts < (mx.DateTime.now() - mx.DateTime.RelativeDateTime(hours=12))):
            time_fmt = "%d %b, %I:%M %p"

        # We have all we need now
        unique_key = "%s_%s_%s_%s_%s_%s" % (gmt_ts, type, city, lat, lon, magf)
        if (lsrdb.has_key(unique_key)):
            logger.info("DUP! %s" % (unique_key,))
            continue
        lsrdb[ unique_key ] = mx.DateTime.gmt()

        jm = "%s:%s [%s Co, %s] %s reports %s %sat %s %s -- %s http://mesonet.agron.iastate.edu/cow/maplsr.phtml?lat0=%s&amp;lon0=-%s&amp;ts=%s" % (wfo, city, cnty, st, source, type, mag_long, ts.strftime(time_fmt), nws.z, remark, lat, lon, gmt_ts.strftime("%Y-%m-%d%%20%H:%M"))
        jmhtml = "%s [%s Co, %s] %s <a href='http://mesonet.agron.iastate.edu/cow/maplsr.phtml?lat0=%s&amp;lon0=-%s&amp;ts=%s'>reports %s %s</a>at %s %s -- %s" % ( city, cnty, st, source, lat, lon, gmt_ts.strftime("%Y-%m-%d%%20%H:%M"), type, mag_long, ts.strftime(time_fmt), nws.z, remark)
        logger.info(jm +"\n")
        sql = "INSERT into lsrs_%s (valid, type, magnitude, city, county, state, \
         source, remark, geom, wfo, typetext) values ('%s+00', '%s', %s, '%s', '%s', '%s', \
         '%s', '%s', 'SRID=4326;POINT(-%s %s)', '%s', '%s')" % \
          (gmt_ts.year, gmt_ts.strftime("%Y-%m-%d %H:%M"), dbtype, mag, city.replace("'","\\'"), re.sub("'", "\\'",cnty), st, source, \
           re.sub("'", "\\'", remark), lon, lat, wfo, type)
        jabber.sendMessage(jm,jmhtml)
        sentMessages += 1
        postgisdb.query(sql)


myJid = jid.JID('iembot_ingest@%s/lsrParse_%s' % (secret.chatserver, mx.DateTime.gmt().ticks() ) )
factory = client.basicClientFactory(myJid, secret.iembot_ingest_password)

jabber = JabberClient(myJid)

factory.addBootstrap('//event/stream/authd',jabber.authd)
factory.addBootstrap("//event/client/basicauth/invaliduser", jabber.debug)
factory.addBootstrap("//event/client/basicauth/authfailed", jabber.debug)
factory.addBootstrap("//event/stream/error", jabber.debug)

reactor.connectTCP(secret.connect_chatserver,5222,factory)

ldm = ldmbridge.LDMProductFactory( myProductIngestor() )
reactor.callLater( 20, cleandb)
reactor.run()

