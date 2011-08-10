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
""" Aviation Product Parser! """

__revision__ = '$Id: spc_parser.py 4513 2009-01-06 16:57:49Z akrherz $'

# Twisted Python imports
from twisted.words.protocols.jabber import client, jid, xmlstream
from twisted.internet import reactor
from twisted.python import log
from twisted.enterprise import adbapi
from twisted.mail import smtp

# Standard Python modules
import re, os, math

# Python 3rd Party Add-Ons
import mx.DateTime, pg

# pyWWA stuff
from support import ldmbridge, TextProduct, utils
import secret
import common

os.environ["EMAILS"] = "10"


log.startLogging(open('logs/aviation.log','a'))
log.FileLogObserver.timeFormat = "%Y/%m/%d %H:%M:%S %Z"

POSTGIS = pg.connect(secret.dbname, secret.dbhost, user=secret.dbuser, 
                     passwd=secret.dbpass)
DBPOOL = adbapi.ConnectionPool("psycopg2", database=secret.dbname, 
                               host=secret.dbhost, password=secret.dbpass)


CS_RE = re.compile(r"""CONVECTIVE\sSIGMET\s(?P<label>[0-9A-Z]+)\n
VALID\sUNTIL\s(?P<hour>[0-2][0-9])(?P<minute>[0-5][0-9])Z\n
(?P<states>[A-Z ]+)\n
FROM\s(?P<locs>[0-9A-Z \-]+)\n
(?P<type>AREA|LINE)(?P<rest>.*)
""", re.VERBOSE )

FROM_RE = re.compile(r"""
(?P<offset>[0-9]+)?(?P<drct>[A-Z]{1,3})?\s?(?P<loc>[A-Z0-9]{3})
""", re.VERBOSE)

OL_RE = re.compile(r"""
OUTLOOK\sVALID\s(?P<begin>[0-9]{6})-(?P<end>[0-9]{6})\n
""", re.VERBOSE)

AREA_RE = re.compile(r"""
AREA\s(?P<areanum>[0-9]+)\.\.\.FROM\s(?P<locs>[0-9A-Z \-]+)\n
""", re.VERBOSE)

# Load LOCS table
LOCS = {}
mesosite = pg.connect('mesosite', secret.dbhost)
rs = mesosite.query("""SELECT id, name, x(geom) as lon, y(geom) as lat from stations 
           WHERE network ~* 'ASOS' or network ~* 'AWOS'""").dictresult()
for i in range(len(rs)):
    LOCS[rs[i]['id']] = rs[i]
mesosite.close()

for line in open('/home/ldm/pyWWA/tables/vors.tbl'):
    if len(line) < 70 or line[0] == '!':
        continue
    id = line[:3]
    lat = float(line[56:60]) / 100.0
    lon = float(line[61:67]) / 100.0
    name = line[16:47].strip()
    LOCS[id] = {'lat': lat, 'lon': lon, 'name': name}

# Finally, GEMPAK!
for line in open('/home/ldm/pyWWA/tables/pirep_navaids.tbl'):
    id = line[:3]
    lat = float(line[56:60]) / 100.0
    lon = float(line[61:67]) / 100.0
    LOCS[id] = {'lat': lat, 'lon': lon}


# LDM Ingestor
class MyProductIngestor(ldmbridge.LDMProductReceiver):
    """ I receive products from ldmbridge and process them 1 by 1 :) """

    def connectionLost(self, reason):
        print 'connectionLost', reason
        reactor.callLater(5, self.shutdown)

    def shutdown(self):
        reactor.callWhenRunning(reactor.stop)


    def process_data(self, buf):
        """ Process the product """
        try:
            prod = TextProduct.TextProduct(buf)
            if prod.afos == 'SIGC':
                process_SIGC(prod)
        except Exception, myexp:
            common.email_error(myexp, buf)

def figure_expire(ptime, hour, minute):
    """
    Convert something like 0255Z into a full blown time
    """
    expire = ptime
    if hour < ptime.hour:
        expire += mx.DateTime.RelativeDateTime(days=1)
    expire += mx.DateTime.RelativeDateTime(hour=hour,minute=minute)
    return expire

def sanitize_angle(val):
    if val < 0:
        return 360 - math.fabs( val )
    if val > 360:
        return val - 360

def makebox(lons, lats):
    """
    Need to make a box from a line, sigh
    if we want a 10km box, we need to go 0.1 in the direction of choice
    hyp = 0.1
    cos(angle) = runx/0.1
    sin(angle) = runy/0.1
    """
    rlats = []
    rlons = []
    deltax = lons[1] - lons[0]
    deltay = lats[1] - lats[0]
    if deltax == 0:
        deltax = 0.001
    angle = math.atan(deltay/deltax)
    runx = 0.1 * math.cos(angle)
    runy = 0.1 * math.sin(angle)
    # UR
    rlons.append( lons[0] - runy )
    rlats.append( lats[0] + runx )
    # UL
    rlons.append( lons[1] - runy )
    rlats.append( lats[1] + runx )
    # LL
    rlons.append( lons[1] + runy )
    rlats.append( lats[1] - runx )
    # LR
    rlons.append( lons[0] + runy )
    rlats.append( lats[0] - runx )
    # UR
    rlons.append( lons[0] - runy )
    rlats.append( lats[0] + runx )
    return rlons, rlats

def locs2lonslats(locstr):
    """
    Convert a locstring into a lon lat arrays
    """
    lats = []
    lons = []
    for l in locstr.split('-'):
        s = FROM_RE.search(l)
        if s:
            d = s.groupdict()
            if d['drct'] is not None:
                 (lon1, lat1) = utils.go2lonlat(LOCS[d['loc']]['lon'], LOCS[d['loc']]['lat'], 
                                                   d['drct'], float(d['offset']) )
            else:
                  (lon1, lat1) = (LOCS[d['loc']]['lon'], LOCS[d['loc']]['lat'])
            lats.append( lat1 )
            lons.append( lon1 )

    if len(lons) == 2:
        (lons, lats) = makebox(lons, lats)

    return lons, lats

def process_SIGC(prod):
    """

    """
    POSTGIS.query("BEGIN;")
    POSTGIS.query("DELETE from sigmets_current where expire < now()")
    for section in prod.raw.split('\n\n'):
        s = CS_RE.search(section)
        if s:
            data = s.groupdict()
            expire = figure_expire(prod.issueTime, float(data['hour']), float(data['minute']))
            lons, lats = locs2lonslats(data['locs'])
            wkt = ""
            for lat,lon in zip(lats,lons):
                wkt += "%s %s," % (lon, lat)
            if lats[0] != lats[-1] or lons[0] != lons[-1]:
                wkt += "%s %s," % (lons[0], lats[0])
            print 'From: %s Till: %s %s' % (prod.issueTime, expire, data['label'])
            print wkt
            for table in ('sigmets_current', 'sigmets_archive'):
                sql = """INSERT into %s(sigmet_type, label, issue, expire, raw, geom)
                   VALUES ('C','%s','%s+00','%s+00','%s',
                   'SRID=4326;MULTIPOLYGON(((%s)))')""" % (table, data['label'], 
                                            prod.issueTime, expire, section, wkt[:-1])
                
                POSTGIS.query(sql)

        """ Gonna punt the outlook for now, no need? 
        s = OL_RE.search(section)
        if s:
            print s.groupdict()
            
        s = AREA_RE.search(section)
        if s:
            data = s.groupdict()
            lons, lats = locs2lonslats(data['locs'])
            wkt = ""
            for lat,lon in zip(lats,lons):
                wkt += "%s %s," % (lon, lat)
            wkt += "%s %s" % (lons[0], lats[0])
        """
    POSTGIS.query("COMMIT;")

myJid = jid.JID('%s@%s/aviation_%s' % (secret.iembot_ingest_user, 
            secret.chatserver, mx.DateTime.gmt().strftime("%Y%m%d%H%M%S") ) )
factory = client.basicClientFactory(myJid, secret.iembot_ingest_password)

jabber = common.JabberClient(myJid)

factory.addBootstrap('//event/stream/authd', jabber.authd)
factory.addBootstrap("//event/client/basicauth/invaliduser", jabber.debug)
factory.addBootstrap("//event/client/basicauth/authfailed", jabber.debug)
factory.addBootstrap("//event/stream/error", jabber.debug)
factory.addBootstrap(xmlstream.STREAM_END_EVENT, jabber._disconnect )

reactor.connectTCP(secret.connect_chatserver, 5222, factory)
ldm = ldmbridge.LDMProductFactory( MyProductIngestor() )
reactor.run()