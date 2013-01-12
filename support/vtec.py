
import re
import datetime
import iemtz

from twisted.python import log

_re = "(/([A-Z])\.([A-Z]+)\.([A-Z]+)\.([A-Z]+)\.([A-Z])\.([0-9]+)\.([0-9,T,Z]+)-([0-9,T,Z]+)/)"

_classDict = {'O': 'Operational',
              'T': 'Test',
              'E': 'Experimental',
              'X': 'Experimental VTEC'}

_actionDict = {'NEW': 'issues',
               'CON': 'continues',
               'EXA': 'extends area of',
               'EXT': 'extends time of',
               'EXB': 'extends area+time of',
               'UPG': 'issues upgrade to',
               'CAN': 'cancels',
               'EXP': 'expires',
               'ROU': 'routine',
               'COR': 'corrects'}

_sigDict = {'W': 'Warning',
            'Y': 'Advisory',
            'A': 'Watch',
            'S': 'Statement',
            'O': 'Outlook',
            'N': 'Synopsis',
            'F': 'Forecast'}

_phenDict = {
'AF': 'Ashfall',
'AS': 'Air Stagnation',
'BH': 'Beach Hazard',
'BS': 'Blowing Snow',
'BW': 'Brisk Wind',
'BZ': 'Blizzard',
'CF': 'Coastal Flood',
'DS': 'Dust Storm',
'DU': 'Blowing Dust',
'EC': 'Extreme Cold',
'EH': 'Excessive Heat',
'EW': 'Extreme Wind',
'FA': 'Areal Flood',
'FF': 'Flash Flood',
'FG': 'Dense Fog',
'FL': 'Flood',
'FR': 'Frost',
'FW': 'Red Flag',
'FZ': 'Freeze',
'GL': 'Gale',
'HF': 'Hurricane Force Wind',
'HI': 'Inland Hurricane',
'HS': 'Heavy Snow',
'HT': 'Heat',
'HU': 'Hurricane',
'HW': 'High Wind',
'HY': 'Hydrologic',
'HZ': 'Hard Freeze',
'IP': 'Sleet',
'IS': 'Ice Storm',
'LB': 'Lake Effect Snow and Blowing Snow',
'LE': 'Lake Effect Snow',
'LO': 'Low Water',
'LS': 'Lakeshore Flood',
'LW': 'Lake Wind',
'MA': 'Marine',
'MF': 'Marine Dense Fog',
'MS': 'Marine Dense Smoke',
'MH': 'Marine Ashfall',
'RB': 'Small Craft for Rough',
'RP': 'Rip Currents',
'SB': 'Snow and Blowing',
'SC': 'Small Craft',
'SE': 'Hazardous Seas',
'SI': 'Small Craft for Winds',
'SM': 'Dense Smoke',
'SN': 'Snow',
'SR': 'Storm',
'SU': 'High Surf',
'SV': 'Severe Thunderstorm',
'SW': 'Small Craft for Hazardous',
'TI': 'Inland Tropical Storm',
'TO': 'Tornado',
'TR': 'Tropical Storm',
'TS': 'Tsunami',
'TY': 'Typhoon',
'UP': 'Ice Accretion',
'WC': 'Wind Chill',
'WI': 'Wind',
'WS': 'Winter Storm',
'WW': 'Winter Weather',
'ZF': 'Freezing Fog',
'ZR': 'Freezing Rain',
}
def contime(s):
    if ( len(re.findall("0000*T",s)) > 0 ): return None
    try:
        ts = datetime.datetime.strptime(s, '%y%m%dT%H%MZ')
        return ts.replace( tzinfo=iemtz.UTC() )
    except Exception, err:
        log.err( err )
        return None
class vtec:

    def __init__(self, raw):
        self.raw = raw
        tokens = re.findall(_re, self.raw)
        if (len(tokens) != 1 or len(tokens[0]) != 9):
            return None
        self.line   = tokens[0][0]
        self.status = tokens[0][1]
        self.action = tokens[0][2]
        self.office = tokens[0][3][1:]
        self.office4 = tokens[0][3]
        self.phenomena = tokens[0][4]
        self.significance = tokens[0][5]
        self.ETN = int(tokens[0][6])
        self.beginTS = contime( tokens[0][7] )
        self.endTS   = contime( tokens[0][8] )

    def url(self, year):
        """ Generate a VTEC url string needed """
        return "%s-%s-%s-%s-%s-%s-%04i" % (year, self.status, self.action,\
               self.office4, self.phenomena, self.significance, self.ETN)

    def __str__(self):
        return self.raw

    def productString(self):
        q = "unknown %s" % (self.action,)
        if (_actionDict.has_key(self.action)):
            q = _actionDict[ self.action ]

        p = "Unknown %s" % (self.phenomena,)
        if (_phenDict.has_key(self.phenomena)):
            p = _phenDict[self.phenomena]

        a = "Unknown %s" % (self.significance,)
        if (_sigDict.has_key(self.significance)):
            a = _sigDict[self.significance]
        # Hack for special FW case
        if (self.significance == 'A' and self.phenomena == 'FW'):
            p = "Fire Weather"
        return "%s %s %s" % (q, p,a)

