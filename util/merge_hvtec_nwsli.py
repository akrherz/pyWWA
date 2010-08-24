# nwsli      | character(5)           | 
# river_name | character varying(128) | 
# proximity  | character varying(16)  | 
# name       | character varying(128) | 
# state      | character(2)           | 
# geom       | geometry               | 

import pg
mydb = pg.connect("postgis")

added = 0
lines = open('hvtec_nwsli.txt')
for line in lines:
  if line.strip() == "":
    continue
  tokens = line.replace("'","\'").split("\t")
  if len(tokens) < 7:
    print line
  (nwsli, river_name, proximity, name, state, lat, lon)  = tokens
  rs = mydb.query("SELECT * from hvtec_nwsli WHERE nwsli = '%s'" % (nwsli,) ).dictresult()
  if len(rs) == 0:
    print "Adding NWSLI: %s" % (nwsli,)
    added += 1
    sql  = "INSERT into hvtec_nwsli (nwsli, river_name, proximity, name, \
         state, geom) values ('%s', '%s', '%s', '%s', '%s', 'SRID=4326;POINT(%s %s)')" % (nwsli, river_name, proximity, name.replace("'", "''"), state, lon, lat)
    mydb.query(sql)

print "Added ", added
