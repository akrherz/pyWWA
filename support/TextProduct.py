
import re, mx.DateTime, string
from support import ugc, vtec, hvtec, reference

#TORNADO = re.compile(r"(STORM\W+CAPABLE\W+OF\W+PRODUCING|REPORTED|INDICATED?)\W+A\W+TORNADO")
TORNADO = re.compile(r"^AT |^\* AT")
WINDHAIL = re.compile(".*WIND\.\.\.HAIL (?P<winddir>[><]?)(?P<wind>[0-9]+)MPH (?P<haildir>[><]?)(?P<hail>[0-9\.]+)IN")
HAILTAG = re.compile(".*HAIL\.\.\.(?P<haildir>[><]?)(?P<hail>[0-9\.]+)IN")
WINDTAG = re.compile(".*WIND\.\.\.(?P<winddir>[><]?)\s?(?P<wind>[0-9]+)\s?MPH")
TORNADOTAG = re.compile(".*TORNADO\.\.\.(?P<tornado>RADAR INDICATED|OBSERVED|POSSIBLE)")
TORNADODAMAGETAG = re.compile(".*TORNADO DAMAGE THREAT\.\.\.(?P<damage>SIGNIFICANT|CATASTROPHIC)")
TIME_MOT_LOC = re.compile(".*TIME\.\.\.MOT\.\.\.LOC (?P<ztime>[0-9]{4})Z (?P<dir>[0-9]{1,3})DEG (?P<sknt>[0-9]{1,3})KT (?P<loc>[0-9 ]+)")

class TextProduct:

    def __init__(self, raw, bypass=False):
        self.idd = None
        self.wmo = None
        self.source = None
        self.issueTime = None
        self.afos = None
        self.segments = []
        self.fcster = None
        self.z = None
        self.product_header = None

        self.raw = raw.strip().replace("\015\015\012", "\n") # to unix
        self.sections = self.raw.split("\n\n")

        if not bypass:     
            parseCallbacks = [self.findIssueTime, self.findSegments, 
                self.findIDD, self.findWMO, self.findAFOS, self.figureFcster, 
                self.find_pheader]
            for cb in parseCallbacks:
                apply( cb )


    def find_pheader(self):
        """ Figure out what the WMO AND MND area and save it for database
            updates where we don't want to save the entire SVS! """
        if (len(self.sections) < 2):
            return

        if len( self.sections[0].split("\n") ) > 3:
            self.product_header = self.sections[0]
        else:
            self.product_header = "\n\n".join(self.sections[0:2])

    def get_iembot_source(self):
        if (self.source is None or len(self.source) != 4):
            return None
        # This is a hack for now, will have to think some more about
        # what to do with the conflict between PABR and KABR
        if (self.source == "PABR"):
            return "XXX"
        return self.source[1:]

    def get_product_id(self):
        s = "%s-%s-%s-%s" % (self.issueTime.strftime("%Y%m%d%H%M"),\
                self.source, self.wmo, self.afos)
        return s.strip()

    def sqlraw(self):
        return re.sub("'", "\\'", self.raw)

    def figureFcster(self):
        self.fcster = string.strip( (self.sections[-1]).replace("\n","") )[:24]

    def findWarnedCounties(self):
        return re.findall("\n  (.*) COUNTY IN (.*)", self.raw)

    def findAFOS(self):
        """ Determine the AFOS header from the NWS text product """
        data = "\n".join([line.strip() 
                         for line in self.sections[0].split("\n")])
        tokens = re.findall("^([A-Z0-9 ]{4,6})$", data, re.M)
        if (len(tokens) > 0):
            self.afos = tokens[0]

    def findWMO(self,):
        t = re.findall("^([A-Z]{4}[0-9][0-9]) ([A-Z]{4})", self.sections[0], re.M)
        if len(t) > 0:
            self.wmo = t[0][0]
            self.source = t[0][1]

    def findIDD(self,):
        t = re.findall("^([0-9]+)", self.sections[0], re.M)
        if (len(t) > 0):
            self.idd = int(t[0])

    def findSegments(self,):
        meat = "\n\n".join( self.sections)
        segs = meat.split("$$")
        for s in segs[:-1]:
            self.segments.append(TextProductSegment(s, self.issueTime))
            

    def __str__(self,):
        s = """
=================================
idd: %(idd)s
wmo: %(wmo)s
source: %(source)s
issueTime: %(issueTime)s
afos: %(afos)s
--------------
""" % vars(self)
        i = 0
        for seg in self.segments:
            s += "+--- segment %s |||| %s" % (i, seg,)
            i += 1
        return s

    def generate_fake_vtec(self):
        """ I generate a fake VTEC ...
            is for non VTEC products prior to 2005, I think :) """
        r = self.raw.replace("NOON", "1200 PM").replace("MIDNIGHT", "1200 AM").replace("VALID UNTIL", "UNTIL")
        dre = "\*\s+UNTIL\s+([0-9]{1,4})\s+(AM|PM)\s+([A-Z]{3,4})"
        tokens = re.findall(dre, r)
        if (len(tokens[0][0]) < 3):
            h = int(tokens[0][0])
            m = 0
        else:
            h = int(tokens[0][0][:-2])
            m = int(tokens[0][0][-2:])
        if tokens[0][1] == "AM" and h == 12:
            h = 0
        if tokens[0][1] == "PM" and h < 12:
            h += 12
        # Figure local issued time
        issue = self.issueTime - mx.DateTime.RelativeDateTime(hours=reference.offsets[self.z])
        expire = issue + mx.DateTime.RelativeDateTime(hour=h, minute=m)
        if (expire < issue):
            expire += mx.DateTime.RelativeDateTime(days=1)
        expireZ = expire + mx.DateTime.RelativeDateTime(hours=reference.offsets[self.z])
        print issue, expire, expireZ, self.z

        if (self.afos[:3] == "SVR"):
            typ = "SV"
        elif (self.afos[:3] == "TOR"):
            typ = "TO"
        elif (self.afos[:3] == "FFW"):
            typ = "FF"
      

        fake = "/O.NEW.%s.%s.W.0000.%s-%s/" % (self.source, typ, \
               self.issueTime.strftime("%y%m%dT%H%MZ"), \
               expireZ.strftime("%y%m%dT%H%MZ") )
        print 'Fake VTEC', fake
        self.segments[0].vtec.append( vtec.vtec(fake) )

    def findIssueTime(self):
        """ Too much fun, we need to determine when this text product was
            issued.  We'll first look to see if it is explicitly said."""

        # Search out the WMO header first, this had better always be there
        # We only care about the first hit in the file, searching from top
        dRE = "^[A-Z0-9]{6} [A-Z]{4} ([0-3][0-9])([0-2][0-9])([0-5][0-9])"
        tokens = re.findall(dRE , self.raw, re.M)
        iDay = int(tokens[0][0])
        iHour = int(tokens[0][1])
        iMinute = int(tokens[0][2])

        # Now lets look for a local timestamp in the product MND or elsewhere
        time_re = "^([0-9]+) (AM|PM) ([A-Z][A-Z][A-Z]?T) [A-Z][A-Z][A-Z] ([A-Z][A-Z][A-Z]) ([0-9]+) ([1-2][0-9][0-9][0-9])$"
        tokens = re.findall(time_re, self.raw, re.M)
        # If we don't find anything, lets default to now, its the best
        if (len(tokens) == 0):
            hack_time = mx.DateTime.utc().strftime("%I%M %p GMT %a %b %d %Y")
            tokens = re.findall(time_re, hack_time.upper(), re.M)
        # [('1249', 'AM', 'EDT', 'JUL', '1', '2005')]
        self.z = tokens[0][2]
        if (len(tokens[0][0]) < 3):
            h = tokens[0][0]
            m = 0
        else:
            h = tokens[0][0][:-2]
            m = tokens[0][0][-2:]
        dstr = "%s:%s %s %s %s %s" % (h, m, tokens[0][1], tokens[0][3], tokens[0][4], tokens[0][5])
        now = mx.DateTime.strptime(dstr, "%I:%M %p %b %d %Y")
        if reference.offsets.has_key(self.z):
            now += mx.DateTime.RelativeDateTime(hours=reference.offsets[self.z])
        else:
            print "Unknown TZ: %s " % (self.z,)

            # Now we need to see if we are changing months!
        addOn = 0
        if (iDay < now.day):
            addOn = mx.DateTime.RelativeDateTime(months=+1)
        self.issueTime = now + mx.DateTime.RelativeDateTime(day=iDay,
                                hour=iHour, minute=iMinute, second=0)

class TextProductSegment:

    def __init__(self, raw, issueTime):
        self.issueTime = issueTime
        self.raw = raw
        self.headlines = []
        self.ugc = []
        self.vtec = []
        self.hvtec = []
        self.bullets = []
        self.giswkt = None
        self.ugcExpire = None
        self.windtag = None
        self.hailtag = None
        self.haildirtag = None
        self.winddirtag = None
        self.tornadotag = None
        self.tornadodamagetag = None
        # TIME...MOT...LOC Stuff!
        self.tml_giswkt = None
        self.tml_valid = None
        self.tml_sknt = None
        self.tml_dir = None
        self.parse()

    def bullet_splitter(self):
        self.bullets = re.findall("\* ([^\*]*)", self.raw.replace("\n"," ") )

    def get_hvtec_nwsli(self):
        if (len(self.hvtec) == 0):
            return ""
        return self.hvtec[0].nwsli

    def svs_search(self):
        """ Special search the product for special text """
        sections = self.raw.split("\n\n")
        for s in sections:
            if len(TORNADO.findall(s)) > 0:
                return " ..."+ re.sub("\s+", " ", s.replace("\n", " "))
        return ""

    def parse(self):
        u = ugc.ugc( self.raw )
        self.ugc = u.ugc
        if (u.rawexpire is not None and self.issueTime is not None):
          day = int(u.rawexpire[:2])
          hr = int(u.rawexpire[2:4])
          mi = int(u.rawexpire[4:])
          offset = 0
          if (day < self.issueTime.day):
              offset = mx.DateTime.RelativeDateTime(months=+1)
          self.ugcExpire = self.issueTime + offset + mx.DateTime.RelativeDateTime(hour=hr, day=day, minute=mi)

        # Lets look for headlines
        self.headlines = re.findall("^\.\.\.(.*?)\.\.\.[ ]?\n\n", self.raw, re.M | re.S)
        for h in range(len(self.headlines)):
          self.headlines[h] = self.headlines[h].replace("...",", ").replace("\n", " ")
        
        # Lets look for VTEC!
        tokens = re.findall(vtec._re, self.raw)
        for t in tokens:
          self.vtec.append( vtec.vtec(t[0]) )

        # Lets look for HVTEC!
        tokens = re.findall(hvtec._re, self.raw)
        for t in tokens:
          self.hvtec.append( hvtec.hvtec(t[0]) )

        s = self.raw.replace("\n", " ")
        m = TIME_MOT_LOC.match( s )
        if m:
            self.process_time_mot_loc(m)

        rend = re.split("LAT\.\.\.LON", s)
        if (len(rend) == 1):
            return None
        # Hack!
        rend2 = re.split("TIME...MOT...LOC", rend[1])
        if (len(rend2) > 1):
            rend[1] = rend2[0]
        pairs = re.findall("([0-9]+)\s+([0-9]+)", rend[1])

        g = "SRID=4326;MULTIPOLYGON((("
        for pr in pairs:
            lat = float(pr[0]) / 100.00
            lon = float(pr[1]) / 100.00
            g += "-%s %s," % (lon, lat)
        g += "-%s %s" % ( float(pairs[0][1]) / 100.00, float(pairs[0][0]) / 100.00)
        g += ")))"
        self.giswkt = g
        # Look for new WIND...HAIL stuff
        if len(rend2) > 1:
            m = WINDHAIL.match( rend2[1] )
            if m:
                d = m.groupdict()
                self.windtag = d['wind']
                self.haildirtag = d['haildir']
                self.winddirtag = d['winddir']
                self.hailtag = d['hail']
                
            m = WINDTAG.match( rend2[1] )
            if m:
                d = m.groupdict()
                self.winddirtag = d['winddir']
                self.windtag = d['wind']
                
            m = HAILTAG.match( rend2[1] )
            if m:
                d = m.groupdict()
                self.haildirtag = d['haildir']
                self.hailtag = d['hail']

            m = TORNADOTAG.match( rend2[1] )
            if m:
                d = m.groupdict()
                self.tornadotag = d['tornado']

            m = TORNADODAMAGETAG.match( rend2[1] )
            if m:
                d = m.groupdict()
                self.tornadodamagetag = d['damage']


            
    def process_time_mot_loc(self, m):
        """
        Process the match results from a find of TIME...MOT...LOC
        """
        d = m.groupdict()
        if len(d['ztime']) != 4 or self.ugcExpire is None:
            return
        hh = float(d['ztime'][:2])
        mi = float(d['ztime'][2:])
        self.tml_valid = self.ugcExpire + mx.DateTime.RelativeDateTime(hour=hh,
                                                              minute=mi)
        if hh > self.ugcExpire.hour:
            self.tml_valid -= mx.DateTime.RelativeDateTime(days=1)

        tokens = d['loc'].split()
        lats = []
        lons = []
        for i in range(0,len(tokens),2):
            lats.append( float(tokens[i]) / 100.0 )
            lons.append( 0 - float(tokens[i+1]) / 100.0 )
        
        if len(lats) == 1:
            self.tml_giswkt = 'SRID=4326;POINT(%s %s)' % (lons[0], lats[0])
        else:
            pairs = []
            for lat,lon in zip(lats,lons):
                pairs.append( '%s %s' % (lon, lat) )
            self.tml_giswkt = 'SRID=4326;LINESTRING(%s)' % (','.join(pairs),)
        self.tml_sknt = float( d['sknt'] )
        self.tml_dir = float( d['dir'] )
        
    def __str__(self,):
        s = """
headlines: %(headlines)s
ugc: %(ugc)s
giswkt: %(giswkt)s
""" % vars(self)
        i = 0
        for v in self.vtec:
            s += "vtec %s: %s\n" % (i, v)
            i += 1
        i = 0
        for h in self.hvtec:
            s += "hvtec %s: %s\n" % (i, h)
            i += 1
        return s

if (__name__ == "__main__"):
    import sys
    o = open( sys.argv[1], 'r').read()
    t = TextProduct(o)
    print t
