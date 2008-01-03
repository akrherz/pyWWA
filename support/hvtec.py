
import re, mx.DateTime

#         nwsli        sev         cause      
_re = "(/([A-Z0-9]{5})\.([N0123U])\.([A-Z]{2})\.([0-9TZ]+)\.([0-9TZ]+)\.([0-9TZ]+)\.([A-Z]{2})/)"

_statusDict = {'00': 'is not applicable',
               'NO': 'is not expected',
               'NR': 'may be expected',
               'UU': 'is not available'}

_causeDict = {'ER': 'Excessive Rainfall',
              'SM': 'Snowmelt',
              'RS': 'Rain and Snowmelt',
              'DM': 'Dam or Levee Failure',
              'GO': 'Glacier-Dammed Lake Outburst',
              'IJ': 'Ice Jam',
              'IC': 'Rain and/or Snowmelt and/or Ice Jam',
              'FS': 'Upstream Flooding plus Storm Surge',
              'FT': 'Upstream Flooding plus Tidal Effects',
              'ET': 'Elevated Upstream Flow plus Tidal Effects',
              'WT': 'Wind and/or Tidal Effects',
              'DR': 'Upstream Dam or Reservoir Release',
              'MC': 'Other Multiple Causes',
              'OT': 'Other Effects',
              'UU': 'Unknown'}

_severityDict = {'N': 'None',
                 '0': 'None',
                 '1': 'Minor',
                 '2': 'Moderate',
                 '3': 'Major',
                 'U': 'Unknown'}


def contime(s):
    if ( len(re.findall("0000*T",s)) > 0 ): return None
    try:
        return mx.DateTime.strptime(s, '%y%m%dT%H%MZ')
    except:
        return None

class hvtec:

    def __init__(self, raw):
        self.raw = raw
        tokens = re.findall(_re, self.raw)
        if (len(tokens) == 0):
            return None
        self.line    = tokens[0][0]
        self.nwsli   = tokens[0][1]
        self.severity = tokens[0][2]
        self.cause = tokens[0][3]
        self.beginTS = contime( tokens[0][4] )
        self.crestTS = contime( tokens[0][5] )
        self.endTS   = contime( tokens[0][6] )
        self.record = tokens[0][7]
        

    def __str__(self):
        return self.raw
