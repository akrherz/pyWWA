# Now that we have all of these fancy warnings, lets make fake IDs!

import sys
from pyIEM import iemdb
i = iemdb.iemdb()
postgis = i['postgis']
yr = sys.argv[1]

touches = {}

def lookup_touches(ugc):
  touches[ ugc ] = []
  sql = """select ugc from nws_ugc WHERE ugc != '%s' and 
        ST_Touches(geom, (select geom from nws_ugc WHERE ugc = '%s' LIMIT 1))""" % (
      ugc, ugc)
  rs = postgis.query( sql ).dictresult()
  for i in range(len(rs)):
    touches[ ugc ].append( rs[i]['ugc'] )


rs = postgis.query("SELECT distinct wfo from warnings_%s" % (yr,)).dictresult()
for i in range(len(rs)):
  wfo = rs[i]['wfo']
  for p in ['TO','SV','FF','MA']:
    rs2 = postgis.query("SELECT issue, expire, count(*) from warnings_%s \
          WHERE wfo = '%s' and phenomena = '%s' and significance = 'W' \
          GROUP by issue,expire ORDER by issue ASC" % (
          yr, wfo, p) ).dictresult()
    idseq = 1
    for j in range(len(rs2)):
      # Simple case of ordering the warnings
      if rs2[j]['count'] == 1: 
        sql = "UPDATE warnings_%s SET eventid = %s WHERE wfo = '%s' and \
           phenomena = '%s' and significance = 'W' and issue = '%s' and \
           expire = '%s'" % (yr, idseq, wfo, p, rs2[j]['issue'], rs2[j]['expire'])
        postgis.query(sql)
        idseq += 1
        continue
      # Uh oh, not so simple, find ugc codes
      sql = """SELECT ugc from warnings_%s WHERE wfo = '%s' and 
           phenomena = '%s' and significance = 'W' and issue = '%s' and 
           expire = '%s'""" % (yr, wfo, p, rs2[j]['issue'], rs2[j]['expire'])
      rs3 = postgis.query( sql ).dictresult()
      ugcs = []
      for z in range(len(rs3)):
        if not touches.has_key( rs3[z]['ugc'] ):
          lookup_touches( rs3[z]['ugc'] )
        ugcs.append( rs3[z]['ugc'] )
      for ugc in ugcs:
        warning = [ugc,]
        for ugc2 in touches[ugc]:
          if ugc2 in ugcs:
            warning.append( ugc2 )
            ugcs.remove( ugc2 )
        print "UGC %s has neighbors %s" % (ugc, str(warning))
        sql = """UPDATE warnings_%s SET eventid = %s WHERE wfo = '%s' and 
           phenomena = '%s' and significance = 'W' and issue = '%s' and 
           expire = '%s' and ugc in %s""" % (yr, idseq, wfo, p, 
           rs2[j]['issue'], rs2[j]['expire'], str(tuple(warning)))
        postgis.query( sql )
        idseq += 1


      #sql = "UPDATE sbw_2004 SET eventid = %s WHERE wfo = '%s' and \
      #     phenomena = '%s' and significance = 'W' and issue = '%s' and \
      #     expire = '%s'" % (j+1, wfo, p, rs2[j]['issue'], rs2[j]['expire'])
      #postgis.query(sql) 
    print wfo, p, len(rs2)
