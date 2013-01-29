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

# Twisted Python imports
from twisted.internet import reactor
from twisted.python import log, logfile
from twisted.enterprise import adbapi

# Standard Python modules
import re, os, math

# Python 3rd Party Add-Ons
import datetime
import pg

# pyWWA stuff
from pyldm import ldmbridge
from pyiem.nws import product
import common

import ConfigParser
config = ConfigParser.ConfigParser()
config.read(os.path.join(os.path.dirname(__file__), 'cfg.ini'))

log.FileLogObserver.timeFormat = "%Y/%m/%d %H:%M:%S %Z"
log.startLogging(logfile.DailyLogFile('aviation.log','logs'))

DBPOOL = adbapi.ConnectionPool("twistedpg", database="postgis", cp_reconnect=True,
                                host=config.get('database','host'), cp_max=1,
                                user=config.get('database','user'),
                                password=config.get('database','password') )

# 
CS_RE = re.compile(r"""CONVECTIVE\sSIGMET\s(?P<label>[0-9A-Z]+)\s
VALID\sUNTIL\s(?P<hour>[0-2][0-9])(?P<minute>[0-5][0-9])Z\s
(?P<states>[A-Z ]+)\s
(?P<from>FROM)?\s?(?P<locs>[0-9A-Z \-]+?)\s
(?P<dmshg>DMSHG|DVLPG|INTSF)?\s?(?P<geotype>AREA|LINE|ISOL)?\s?
(?P<cutype>EMBD|SEV|SEV\sEMBD|EMBD\sSEV)?\s?TS\s(?P<width>[0-9]+\sNM\sWIDE)?(?P<diameter>D[0-9]+)?
""", re.VERBOSE )

FROM_RE = re.compile(r"""
(?P<offset>[0-9]+)?(?P<drct>N|NE|NNE|ENE|E|ESE|SE|SSE|S|SSW|SW|WSW|W|WNW|NW|NNW)?\s?(?P<loc>[A-Z0-9]{3})
""", re.VERBOSE)

OL_RE = re.compile(r"""
OUTLOOK\sVALID\s(?P<begin>[0-9]{6})-(?P<end>[0-9]{6})\n
""", re.VERBOSE)

AREA_RE = re.compile(r"""
AREA\s(?P<areanum>[0-9]+)\.\.\.FROM\s(?P<locs>[0-9A-Z \-]+)\n
""", re.VERBOSE)

# Load LOCS table
LOCS = {}
mesosite = pg.connect('mesosite', config.get('database','host'), 
                      passwd=config.get('database','password'))
rs = mesosite.query("""SELECT id, name, x(geom) as lon, y(geom) as lat from stations 
           WHERE network ~* 'ASOS' or network ~* 'AWOS'""").dictresult()
for i in range(len(rs)):
    LOCS[rs[i]['id']] = rs[i]
mesosite.close()

for line in open('/home/ldm/pyWWA/tables/vors.tbl'):
    if len(line) < 70 or line[0] == '!':
        continue
    sid = line[:3]
    lat = float(line[56:60]) / 100.0
    lon = float(line[61:67]) / 100.0
    name = line[16:47].strip()
    LOCS[sid] = {'lat': lat, 'lon': lon, 'name': name}

# Finally, GEMPAK!
for line in open('/home/ldm/pyWWA/tables/pirep_navaids.tbl'):
    sid = line[:3]
    lat = float(line[56:60]) / 100.0
    lon = float(line[61:67]) / 100.0
    LOCS[sid] = {'lat': lat, 'lon': lon}


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
            prod = product.TextProduct(buf)
            if prod.afos in ['SIGC','SIGW','SIGE']:
                defer = DBPOOL.runInteraction(process_SIGC, prod)
                defer.addErrback(common.email_error, buf)
            elif prod.afos[:2] == 'WS':
                defer = DBPOOL.runInteraction(process_WS, prod)
                defer.addErrback(common.email_error, buf)
        except Exception, myexp:
            common.email_error(myexp, buf)

def figure_expire(ptime, hour, minute):
    """
    Convert something like 0255Z into a full blown time
    """
    expire = ptime
    if hour < ptime.hour:
        expire += datetime.timedelta(days=1)
    return expire.replace(hour=hour,minute=minute)
    
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
dirs = {'NNE': 22.5, 'ENE': 67.5, 'NE':  45.0, 'E': 90.0, 'ESE': 112.5,
        'SSE': 157.5, 'SE': 135.0, 'S': 180.0, 'SSW': 202.5,
        'WSW': 247.5, 'SW': 225.0, 'W': 270.0, 'WNW': 292.5,
        'NW': 315.0, 'NNW': 337.5, 'N': 0, '': 0}

KM_SM = 1.609347


def go2lonlat(lon0, lat0, direction, displacement):
    x = -math.cos( math.radians( dirs[direction] ) )
    y = math.sin( math.radians( dirs[direction] ) )
    lat0 += (y * displacement * KM_SM / 111.11 )
    lon0 += (x * displacement * KM_SM /(111.11*math.cos( math.radians(lat0))))

    return lon0, lat0


def locs2lonslats(locstr, geotype, widthstr, diameterstr):
    """
    Convert a locstring into a lon lat arrays
    """
    lats = []
    lons = []
    #if geotype == 'LINE':
    #    width = float(widthstr.replace(" NM WIDE", ""))
        # Approximation
    #    widthdeg = width / 110.

    #log.msg("locstr is:%s geotype is:%s", (locstr, geotype))
    for l in locstr.split('-'):
        s = FROM_RE.search(l)
        if s:
            d = s.groupdict()
            if d['offset'] is not None:
                (lon1, lat1) = go2lonlat(LOCS[d['loc']]['lon'], LOCS[d['loc']]['lat'], 
                                                   d['drct'], float(d['offset']) )
            else:
                (lon1, lat1) = (LOCS[d['loc']]['lon'], LOCS[d['loc']]['lat'])
            lats.append( lat1 )
            lons.append( lon1 )
    if geotype == 'ISOL' or diameterstr is not None:
        lats2 = []
        lons2 = []
        diameter = float(diameterstr.replace("D", ""))
        # Approximation
        diameterdeg = diameter / 110.
        # UR
        lons2.append( lons[0] - diameterdeg )
        lats2.append( lats[0] + diameterdeg )
        # UL
        lons2.append( lons[0] + diameterdeg )
        lats2.append( lats[0] + diameterdeg )
        # LL
        lons2.append( lons[0] + diameterdeg )
        lats2.append( lats[0] - diameterdeg )
        # LR
        lons2.append( lons[0] - diameterdeg )
        lats2.append( lats[0] - diameterdeg )
        lons = lons2
        lats = lats2
        
    if geotype == 'LINE':
        lats2 = []
        lons2 = []
        # Figure out left hand points
        for i in range(0, len(lats)-1):
            deltax = lons[i+1] - lons[i]
            deltay = lats[i+1] - lats[i]
            if deltax == 0:
                deltax = 0.001
            angle = math.atan(deltay/deltax)
            runx = 0.1 * math.cos(angle)
            runy = 0.1 * math.sin(angle)
            # UR
            lons2.append( lons[i] - runy )
            lats2.append( lats[i] + runx )
            # UL
            lons2.append( lons[i+1] - runy )
            lats2.append( lats[i+1] + runx )
            
        for i in range(0, len(lats)-1):
            deltax = lons[i+1] - lons[i]
            deltay = lats[i+1] - lats[i]
            if deltax == 0:
                deltax = 0.001
            angle = math.atan(deltay/deltax)
            runx = 0.1 * math.cos(angle)
            runy = 0.1 * math.sin(angle)
            # LL
            lons2.append( lons[i+1] + runy )
            lats2.append( lats[i+1] - runx )
            # LR
            lons2.append( lons[i] + runy )
            lats2.append( lats[i] - runx )

        lons = lons2
        lats = lats2

    return lons, lats

def process_WS(txn, prod):
    """
    Process non-convective sigmet WS[1-6][N-Y]
    """
    pass

def process_SIGC(txn, prod):
    """

    """
    txn.execute("DELETE from sigmets_current where expire < now()")
    for section in prod.unixtext.split('\n\n'):
        #log.msg("SECTION IS: "+ section.replace("\n", ' '))
        s = CS_RE.search(section.replace("\n", ' '))
        if s:
            data = s.groupdict()
            expire = figure_expire(prod.valid, float(data['hour']), float(data['minute']))
            lons, lats = locs2lonslats(data['locs'], data['geotype'], data['width'], data['diameter'])
            wkt = ""
            for lat,lon in zip(lats,lons):
                wkt += "%s %s," % (lon, lat)
            if lats[0] != lats[-1] or lons[0] != lons[-1]:
                wkt += "%s %s," % (lons[0], lats[0])
            print '%s %s From: %s Till: %s Len(lats): %s' % (data['label'], data['geotype'], 
                                    prod.valid, expire, len(lats))
            for table in ('sigmets_current', 'sigmets_archive'):
                sql = "DELETE from "+table+" where label = %s and expire = %s"
                args = (data['label'], expire)
                txn.execute(sql, args)
                sqlwkt = "SRID=4326;MULTIPOLYGON(((%s)))" % (wkt[:-1],)
                sql = """INSERT into """+table+"""(sigmet_type, label, issue, 
                    expire, raw, geom) VALUES ('C',%s, %s, %s, %s,
                   %s)""" 
                args = (data['label'], 
                                        prod.valid, expire, section, sqlwkt)
                txn.execute(sql, args)

        elif section.find("CONVECTIVE SIGMET") > -1:
            if section.find("CONVECTIVE SIGMET...NONE") == -1:
                common.email_error("Couldn't parse section", section)
                
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

jabber = common.make_jabber_client("aviation")
ldm = ldmbridge.LDMProductFactory( MyProductIngestor() )
reactor.run()