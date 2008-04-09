
import re, mx.DateTime, string
from support import ugc, vtec, hvtec, reference

TORNADO = re.compile(r"(STORM\W+CAPABLE\W+OF\W+PRODUCING|REPORTED|INDICATED?)\W+A\W+TORNADO")

class TextProduct:

    def __init__(self, raw):
        self.idd = None
        self.wmo = None
        self.source = None
        self.issueTime = None
        self.afos = None
        self.segments = []
        self.fcster = None
        self.z = None

        self.raw = raw.strip().replace("\015\015\012", "\n") # to unix
        self.sections = self.raw.split("\n\n")
        
        parseCallbacks = [self.findIssueTime, self.findSegments, self.findIDD,
            self.findWMO, self.findAFOS, self.figureFcster]
        for cb in parseCallbacks:
            apply( cb )


    def get_iembot_source(self):
        if (self.source is None or len(self.source) != 4): return None
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
        if (len(t) == 1):
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
        self.giswkt = None
        self.ugcExpire = None

        self.parse()

    def svs_search(self):
        """ Special search the product for special text """
        sections = self.raw.split("\n\n")
        for s in sections:
            if len(TORNADO.findall(s)) > 0:
                return " ..."+ s.replace("\n", " ")
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
        rend = re.split("LAT\.\.\.LON", s)
        if (len(rend) == 1):
            return None
        # Hack!
        rend2 = re.split("TIME...MOT...LOC", rend[1])
        if (len(rend2) > 1):
            rend[1] = rend2[0]
        pairs = re.findall("([0-9]+) ([0-9]+)", rend[1])

        g = "SRID=4326;MULTIPOLYGON((("
        for pr in pairs:
            lat = float(pr[0]) / 100.00
            lon = float(pr[1]) / 100.00
            g += "-%s %s," % (lon, lat)
        g += "-%s %s" % ( float(pairs[0][1]) / 100.00, float(pairs[0][0]) / 100.00)
        g += ")))"
        self.giswkt = g

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
