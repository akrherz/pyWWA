'''
 Hit the NWSChat quasi web service to get the text IEM may have missed :(
 
 https://nwschat.weather.gov/vtec/json-text.php?
 year=2014&wfo=DDC&phenomena=SV&eventid=0020&significance=W



'''

import urllib2
import sys
import json

year         = sys.argv[1]
wfo          = sys.argv[2]
phenomena    = sys.argv[3]
significance = sys.argv[4]
eventid      = sys.argv[5]

def wrap(data):
    ''' convert data into more noaaportish '''
    return "\001" + data.replace("\n", "\r\r\n") +"\003"

def process(j):
    ''' Process the json data j '''
    if len(j['data']) == 0:
        print 'ERROR: No results found!'
        return
    
    out = open('/tmp/vtec_data.txt', 'w')
    out.write( wrap( j['data'][0]['report'] ) )
    for svs in j['data'][0]['svs']:
        out.write( wrap( svs ) )
        
    out.close()
        

if __name__ == '__main__':
    
    uri = ("https://nwschat.weather.gov/vtec/json-text.php?"+
           "year=%s&wfo=%s&phenomena=%s&eventid=%s&significance=%s") % (
            year, wfo, phenomena, eventid, significance)
           
    data = urllib2.urlopen(uri).read()
    j = json.loads(data)
    
    process(j)