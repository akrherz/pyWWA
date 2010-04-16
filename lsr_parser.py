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
""" LSR product ingestor """

# Get the logger going, asap
from twisted.python import log
log.startLogging(open('logs/lsrParse.log', 'a'))
log.FileLogObserver.timeFormat = "%Y/%m/%d %H:%M:%S %Z"

# Standard python imports
import re, pickle
from email.MIMEText import MIMEText

# Third party python stuff
import mx.DateTime
from twisted.enterprise import adbapi
from twisted.mail import smtp
from twisted.words.protocols.jabber import client, jid
from twisted.internet import reactor

# IEM python Stuff
import common
import secret
from support import TextProduct,  ldmbridge, reference

DBPOOL = adbapi.ConnectionPool("psycopg2", database=secret.dbname, host=secret.dbhost, password=secret.dbpass)
EMAILS = 10

class ProcessingException(Exception):
    """ Generic Exception for processing errors I can handle"""
    pass

# Cheap datastore for LSRs to avoid Dups!
LSRDB = {}
try:
    LSRDB = pickle.load( open('lsrdb.p') )
except:
    log.msg("Error Loading LSRBD")

def cleandb():
    """ To keep LSRDB from growing too big, we clean it out 
        Lets hold 7 days of data!
    """
    thres = mx.DateTime.gmt() - mx.DateTime.RelativeDateTime(hours=24*7)
    init_size = len(LSRDB.keys())
    for key in LSRDB.keys():
        if (LSRDB[key] < thres):
            del LSRDB[key]

    fin_size = len(LSRDB.keys())
    log.msg("cleandb() init_size: %s final_size: %s" % (init_size, fin_size))
    # Non blocking hackery
    reactor.callInThread(pickledb)

    # Call Again in 30 minutes
    reactor.callLater(60*30, cleandb) 

def pickledb():
    """ Dump our database to a flat file """
    pickle.dump(LSRDB, open('lsrdb.p','w'))

def email_error(message, product_text, lsr=None):
    """
    Generic something to send email error messages 
    """
    global EMAILS
    log.msg( message )
    EMAILS -= 1
    if (EMAILS < 0):
        return

    msg = MIMEText("Exception:\n%s\n\nRaw Product:\n%s\n%s" \
                 % (message, product_text, lsr))
    msg['subject'] = 'lsr_parser.py Traceback'
    msg['From'] = secret.parser_user
    msg['To'] = 'akrherz@iastate.edu'
    smtp.sendmail("mailhub.iastate.edu", msg["From"], msg["To"], msg)


class MyProductIngestor(ldmbridge.LDMProductReceiver):
    """ I process products handed off to me from faithful LDM """

    def process_data(self, buf):
        """ @buf: String that is a Text Product """
        if (len(buf) < 50):
            log.msg("Too short LSR product found:\n%s" % (buf,))
            return

        raw = buf.replace("\015\015\012", "\n")
        try:
            nws = TextProduct.TextProduct(raw)
            real_processor(nws)
        except ProcessingException, msg:
            send_iemchat_error(nws, msg)
        except Exception,myexp:
            email_error(myexp, buf, nws)

    def connectionLost(self, reason):
        print 'connectionLost', reason
        reactor.callLater(5, self.shutdown)

    def shutdown(self):
        reactor.callWhenRunning(reactor.stop)



def send_iemchat_error(nws, msgtxt):
    """ Send an error message to the chatroom and to daryl """

    msg = "%s: iembot processing error\nProduct: %s\nError: %s" % \
            (nws.get_iembot_source(), \
             nws.get_product_id(), msgtxt )

    htmlmsg = """<span style='color: #FF0000; font-weight: bold;'>
iembot processing error</span><br/>Product: %s<br/>Error: %s""" % \
            (nws.get_product_id(), msgtxt )
    jabber.sendMessage(msg, htmlmsg)

class LSR:
    """ Object to hold a LSR and be more 00 with things """

    def __init__(self):
        """ Constructor """
        self.lts = None
        self.gts = None
        self.typetext = None
        self.lat = 0
        self.lon = 0
        self.city = None
        self.county = None
        self.source = None
        self.remark = None
        self.magf = None
        self.mag = None
        self.state = None
        self.source = None
        self.raw = None

    def __str__(self):
        return self.raw

    def consume_lines(self, line1, line2):
        """ Consume the first line of a LSR statement """
        #0914 PM     HAIL             SHAW                    33.60N 90.77W
        #04/29/2005  1.00 INCH        BOLIVAR            MS   EMERGENCY MNGR
        self.raw = "%s\n%s" % (line1, line2)

        time_parts = re.split(" ", line1)
        hour12 = time_parts[0][:-2]
        minute = time_parts[0][-2:]
        ampm = time_parts[1]
        dstr = "%s:%s %s %s" % (hour12, minute, ampm, line2[:10])
        self.lts = mx.DateTime.strptime(dstr, "%I:%M %p %m/%d/%Y")

        self.typetext = (line1[12:29]).strip().upper()
        self.city = (line1[29:53]).strip().title()

        lalo = line1[53:]
        tokens = lalo.strip().split()
        self.lat = tokens[0][:-1]
        self.lon = tokens[1][:-1]

        self.magf = (line2[12:29]).strip()
        self.mag = re.sub("(ACRE|INCHES|INCH|MILE|MPH|KTS|U|FT|F|E|M|TRACE)", "", self.magf)
        if (self.mag == ""):
            self.mag = 0
        print "mag: |%s| magf: |%s|" % (self.mag, self.magf)
        self.county = (line2[29:48]).strip().title()
        self.state = line2[48:50]
        self.source = (line2[53:]).strip().lower()

    def mag_string(self):
        """ Create a nice string representation of LSR magnitude """
        mag_long = ""
        if (self.typetext == "HAIL" and \
            reference.hailsize.has_key(float(self.mag))):
            haildesc = reference.hailsize[float(self.mag)]
            mag_long = "of %s size (%s) " % (haildesc, self.magf)
        elif (self.mag != 0):
            mag_long = "of %s " % (self.magf,)

        return mag_long

    def gen_unique_key(self):
        """ Generate an unique key to store stuff """
        return "%s_%s_%s_%s_%s_%s" % (self.gts, self.typetext, \
                self.city, self.lat, self.lon, self.magf)

    def set_gts(self, tsoff):
        """ Set GTS via an offset """
        self.gts = self.lts + tsoff

    def url_builder(self, wfo):
        """ URL builder """
        #uri = secret.MAP_LSR
        #uri += "?lat0=%s&amp;lon0=-%s&amp;ts=%s&amp;wfo=%s" % \
        #       (self.lat,self.lon,self.gts.strftime("%Y-%m-%d%%20%H:%M"),\
        #        wfo)
        uri =  "%s#%s/%s/%s" % (secret.MAP_LSR, wfo, 
                self.gts.strftime("%Y%m%d%H%M"),
                self.gts.strftime("%Y%m%d%H%M") )
        return uri


def real_processor(nws):
    """ Lets actually process! """
    wfo = nws.get_iembot_source()

    tsoff = mx.DateTime.RelativeDateTime(hours= reference.offsets[nws.z])

    goodies = "\n".join( nws.sections[3:] )
    data = re.split("&&", goodies)
    lines = re.split("\n", data[0])

    _state = 0
    i = 0
    time_floor = mx.DateTime.now() - mx.DateTime.RelativeDateTime(hours=12)
    min_time = mx.DateTime.DateTime(2040, 1, 1)
    max_time = mx.DateTime.DateTime(1970, 1, 1)
    duplicates = 0
    new_reports = 0
    while (i < len(lines)):
        # Line must start with a number?
        if (len(lines[i]) < 40 or (re.match("[0-9]", lines[i][0]) == None)):
            i += 1
            continue
        # We can safely eat this line
        lsr = LSR()
        lsr.consume_lines( lines[i], lines[i+1])
        lsr.set_gts(tsoff)
        if (lsr.gts > max_time):
            max_time = lsr.gts
        if (lsr.gts < min_time):
            min_time = lsr.gts

        i += 1

        # Now we search
        searching = 1
        remark = ""
        while (searching):
            i += 1
            if (len(lines) == i):
                break
            #print i, lines[i], len(lines[i])
            if (len(lines[i]) == 0 or [" ", "\n"].__contains__(lines[i][0]) ):
                remark += lines[i]
            else:
                break

        remark = remark.lower().strip()
        remark = re.sub("[\s]{2,}", " ", remark)
        remark = remark.replace("&", "&amp;")
        remark = remark.replace(">", "&gt;").replace("<","&lt;")

        lsr.remark = remark

        if not reference.lsr_events.has_key(lsr.typetext):
            errmsg = "Unknown LSR typecode '%s'" % (lsr.typetext,)
            raise ProcessingException, errmsg

        dbtype = reference.lsr_events[lsr.typetext]
        time_fmt = "%I:%M %p"
        if (lsr.lts < time_floor):
            time_fmt = "%d %b, %I:%M %p"

        # We have all we need now
        unique_key = lsr.gen_unique_key()

        if (LSRDB.has_key(unique_key)):
            log.msg("DUP! %s" % (unique_key,))
            duplicates += 1
            continue
        new_reports += 1
        LSRDB[ unique_key ] = mx.DateTime.gmt()

        mag_long = lsr.mag_string()
        uri = lsr.url_builder(wfo)

        jabber_text = "%s: %s [%s Co, %s] %s reports %s %sat %s %s -- %s %s" % \
             (wfo, lsr.city, lsr.county, lsr.state, lsr.source, \
              lsr.typetext, mag_long, \
              lsr.lts.strftime(time_fmt), nws.z, lsr.remark, uri)
        jabber_html = \
          "%s: %s [%s Co, %s] %s <a href='%s'>reports %s %s</a>at %s %s -- %s" % \
          (wfo,lsr.city, lsr.county, lsr.state, lsr.source, uri, lsr.typetext, \
           mag_long, lsr.lts.strftime(time_fmt), nws.z, lsr.remark)
        jabber.sendMessage(jabber_text, jabber_html)
        twt = "%s [%s Co, %s] %s reports %s %sat %s %s" % (lsr.city, lsr.county, lsr.state, lsr.source, 
              lsr.typetext, mag_long, 
              lsr.lts.strftime(time_fmt), nws.z)
        common.tweet([wfo,], twt, uri, {'lat': lsr.lat, 'long': lsr.lon})

        sql = "INSERT into lsrs_%s (valid, type, magnitude, city, \
               county, state, source, remark, geom, wfo, typetext) \
               values ('%s+00', '%s', %s, '%s', '%s', '%s', \
               '%s', '%s', 'SRID=4326;POINT(-%s %s)', '%s', '%s')" % \
          (lsr.gts.year, lsr.gts.strftime("%Y-%m-%d %H:%M"), dbtype, lsr.mag, \
           lsr.city.replace("'","\\'"), re.sub("'", "\\'",lsr.county), \
           lsr.state, lsr.source, \
           re.sub("'", "\\'", lsr.remark), lsr.lon, lsr.lat, wfo, lsr.typetext)

        DBPOOL.runOperation(sql).addErrback( email_error, sql, lsr)

    if (nws.raw.find("...SUMMARY") > 1):
        extra_text = ""
        if (duplicates > 0):
            extra_text = ", %s out of %s reports were previously \
sent and not repeated here." % (duplicates, duplicates + new_reports)

        #uri = secret.MAP_LSR
        #uri += "?lat0=%s&amp;lon0=-%s&amp;ts=%s&amp;ts2=%s&amp;wfo=%s" % \
        #     (lsr.lat,lsr.lon,min_time.strftime("%Y-%m-%d%%20%H:%M"),\
        #      max_time.strftime("%Y-%m-%d%%20%H:%M"), wfo )
        uri =  "%s#%s/%s/%s" % (secret.MAP_LSR, wfo, 
               min_time.strftime("%Y%m%d%H%M"),
               max_time.strftime("%Y%m%d%H%M") )
        jabber_text = "%s: %s issues Summary Local Storm Report %s %s" % \
           (wfo, wfo, extra_text, uri)
        jabber_html = "%s issues <a href='%s'>Summary Local Storm Report</a>%s"\
            % (wfo, uri, extra_text)
        jabber.sendMessage(jabber_text, jabber_html)
        twt = "Summary Local Storm Report"
        common.tweet([wfo,], twt, uri)

myJid = jid.JID('%s@%s/lsr_parse_%s' % \
      (secret.iembot_ingest_user, secret.chatserver, \
       mx.DateTime.gmt().strftime("%Y%m%d%H%M%S") ) )
factory = client.basicClientFactory(myJid, secret.iembot_ingest_password)

jabber = common.JabberClient(myJid)

factory.addBootstrap('//event/stream/authd', jabber.authd)
factory.addBootstrap("//event/client/basicauth/invaliduser", jabber.debug)
factory.addBootstrap("//event/client/basicauth/authfailed", jabber.debug)
factory.addBootstrap("//event/stream/error", jabber.debug)

reactor.connectTCP(secret.connect_chatserver, 5222, factory)

LDM = ldmbridge.LDMProductFactory( MyProductIngestor() )
reactor.callLater( 20, cleandb)
reactor.run()

