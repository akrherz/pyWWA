# Need to check the log files for shef parser and create new sites 
# within database based on what we find

import re, iemdb, os
MESOSITE = iemdb.connect('mesosite', bypass=False)


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
                          'skip': False,
                          }

    
# Look for sites 
for line in open('/home/ldm/logs/shef_parser.log'):
    tokens = re.findall("stationID: ([A-Z1-9]{5}) ", line)
    if len(tokens) == 0:
        continue
    nwsli = tokens[0]
    if not sites.has_key(nwsli):
        print 'MISSING %s' % (nwsli,)
        sites[nwsli] = {'skip': True}
        continue
    if sites[nwsli]['skip']:
        continue
    sites[nwsli]['skip'] = True
    # Now, we insert
    mcursor = MESOSITE.cursor()
    gtxt = 'SRID=4326;POINT(%s %s)' % (sites[nwsli]['lon'], sites[nwsli]['lat'])
    try:
        mcursor.execute("""
    INSERT into stations(id, name, state, country, network, online, geom) VALUES
    (%s, %s, %s, 'US', %s, 't', %s)
    """, (nwsli, sites[nwsli]['name'], sites[nwsli]['state'],
          '%s_COOP' % (sites[nwsli]['state'],), gtxt))
        mcursor.close()
    except:
        pass
    MESOSITE.commit()
    
    cmd = "/usr/bin/env python /var/www/scripts/util/addSiteMesosite.py %s_COOP %s" % (sites[nwsli]['state'], nwsli)
    os.system(cmd)
    print 'Added %s [%s]' % (nwsli, sites[nwsli]['name'])
    
MESOSITE.commit()