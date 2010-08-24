
from osgeo import ogr
import pg
"""
 gid           | integer                | not null default nextval('nws_ugc_gid_
seq'::regclass)
 polygon_class | character varying(1)   | 
 ugc           | character varying(6)   | 
 name          | character varying(238) | 
 state         | character varying(2)   | 
 time_zone     | character varying(2)   | 
 wfo           | character varying(9)   | 
 fe_area       | character varying(2)   | 
 geom          | geometry               | 
 centroid      | geometry               | 
"""

postgis = pg.connect('postgis')

f = ogr.Open('z_11my10.shp')
GEO_TYP = 'Z'
lyr = f.GetLayer(0)

feat = lyr.GetNextFeature()
while feat is not None:
  state = feat.GetField('STATE')
  zone  = feat.GetField('ZONE')
  if state is None or zone is None:
    print "Skipping record due to nulls"
    feat = lyr.GetNextFeature()
    continue
  name  = feat.GetField('NAME') 

  geo = feat.GetGeometryRef()
  wkt = geo.ExportToWkt()

  ugc = "%s%s%s" % (state, GEO_TYP, zone)

  rs = postgis.query("""SELECT * from nws_ugc WHERE ugc = '%s' and 
       geom = ST_SetSRID(ST_GeomFromEWKT('%s'),4326)
       and wfo = '%s'""" % (
       ugc, wkt, feat.GetField('CWA') ) ).dictresult()
  if len(rs) == 0:
    postgis.query("DELETE from nws_ugc WHERE ugc = '%s'" % (ugc,))
    sql = """INSERT into nws_ugc (polygon_class, ugc, name, state, wfo,
          time_zone, fe_area, geom, centroid) VALUES ('%s','%s','%s','%s','%s',
          '%s',  %s, ST_Multi(ST_SetSRID(ST_GeomFromEWKT('%s'),4326)),
          ST_Centroid( ST_SetSRID(ST_GeomFromEWKT('%s'),4326) ) )""" % (
          GEO_TYP, ugc, name.replace("'", " "), state, feat.GetField('CWA'),
          feat.GetField('TIME_ZONE'), feat.GetFieldAsInteger('FE_AREA'), wkt,
          wkt )
    print 'Updating ', name
    postgis.query(sql)


  feat = lyr.GetNextFeature()
