import urllib2
import json
from pyiem.network import Table as NetworkTable

ph = "FF"
sig = "A"
o = open('prods_%s_%s.csv' % (ph, sig), 'w')
o.write(("WFO,NWSLI,EVENTID,PHENOMENA,SIGNIFICANCE,ISSUE,PRODUCT_ISSUE,"
         "INIT_EXPIRE,EXPIRE,AREA,LOCS\n"))
nt = NetworkTable("WFO")
for year in range(2005, 2016):
    print year
    for wfo in nt.sts.keys():
        uri = ("http://mesonet.agron.iastate.edu/vtec/json-list.php?"
               "year=%s&wfo=%s&phenomena=%s&significance=%s"
               ) % (year, wfo, ph, sig)
        j = json.loads(urllib2.urlopen(uri).read())
        for p in j['products']:
            o.write(("%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n"
                     ) % (wfo, p['nwsli'],
                          p['eventid'], p['phenomena'], p['significance'],
                          p['iso_issued'], p['iso_product_issued'],
                          p['iso_init_expired'],
                          p['iso_expired'], p['area'], p['locations']))
o.close()
