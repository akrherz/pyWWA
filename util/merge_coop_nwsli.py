# Need to check the log files for shef parser and create new sites 
# within database based on what we find

import re, iemdb, os, getpass, urllib2, base64, re
MESOSITE = iemdb.connect('mesosite', bypass=False)
HADS = iemdb.connect('hads', bypass=False)
hcursor = HADS.cursor()
hcursor2 = HADS.cursor()

#darylpass = getpass.getpass("PASS?")

# Load up our database
sites = {}
for line in open('coop_nwsli.txt'):
    tokens = line.split("|")
    if len(tokens) < 9:
        continue
    name = tokens[4]
    if name == "":
        name = tokens[1]
    
    sites[ tokens[0] ] = {'name': name.replace("'", ''),
                          'lat': tokens[6],
                          'lon': tokens[7],
                          'state': tokens[8], 
                          'program': tokens[15],
                          'skip': False,
                          }

def ask_nws(nwsli):
    
    URL = "https://ops13jweb.nws.noaa.gov/nwsli/liu/AsciiReport.jsp?v_sid=%s&v_wfo=&v_city=&v_county=&v_state=&v_region=&v_declat=&v_declon=&v_pgma=&v_pid=&v_owner=&orderBy=sid#" % (nwsli,)
    username = 'daryl.herzmann'
    req = urllib2.Request(URL)
    
    base64string = base64.encodestring(
                '%s:%s' % (username, darylpass))[:-1]
    authheader =  "Basic %s" % base64string
    req.add_header("Authorization", authheader)
    data = urllib2.urlopen(req).read()
    print re.findall("<pre>(*)</pre>")
    sys.exit()

# Look for sites 
hcursor.execute("""SELECT nwsli, product, network from unknown""")
for row in hcursor:
    nwsli = row[0]
    if not sites.has_key(nwsli):
        print 'MISSING %s' % (nwsli,)
        #ask_nws(nwsli)
        sites[nwsli] = {'skip': True}
        continue
    if sites[nwsli]['skip']:
        continue
    sites[nwsli]['skip'] = True
    
    if row[2].find("COOP") > -1 and sites[nwsli]['program'].find("COOP") > -1:
        network = row[2]
    elif row[2].find("DCP") > -1 and sites[nwsli]['program'].find("COOP") == -1:
        network = row[2]
    else:
        print 'CONFLICT [%s] Program [%s] Parser [%s] %s' % (nwsli, 
                    sites[nwsli]['program'], row[2], row[1])
        continue
    # Now, we insert
    mcursor = MESOSITE.cursor()
    gtxt = 'SRID=4326;POINT(%s %s)' % (sites[nwsli]['lon'], sites[nwsli]['lat'])
    
    mcursor.execute("""
    INSERT into stations(id, name, state, country, network, online, geom) VALUES
    (%s, %s, %s, 'US', %s, 't', %s)
    """, (nwsli, sites[nwsli]['name'][:40], sites[nwsli]['state'],
          network, gtxt))
    mcursor.close()
    MESOSITE.commit()
    hcursor2.execute("""DELETE from unknown where nwsli = '%s'""" % (nwsli,))
    
    cmd = "/usr/bin/env python /var/www/scripts/util/addSiteMesosite.py %s %s" % (network, nwsli)
    os.system(cmd)
    print 'Added %s %s [%s]' % (nwsli, network, sites[nwsli]['name'])
    
MESOSITE.commit()
HADS.commit()
#os.unlink('/home/ldm/logs/shef_parser.log')
#os.system('kill -1 `cat /home/ldm/shef_parser.pid`')