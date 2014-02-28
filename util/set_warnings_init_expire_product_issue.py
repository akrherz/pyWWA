import psycopg2
from pyiem.nws import product
from pyiem.nws.ugc import UGC
import sys

POSTGIS = psycopg2.connect(database='postgis', host='iemdb')
cursor = POSTGIS.cursor()
cursor2 = POSTGIS.cursor()

table = "warnings_%s" % (sys.argv[1],)

cursor.execute("""
 SELECT oid, report, ugc from """+table+""" where phenomena = 'BZ'
 and significance = 'A' and product_issue is null LIMIT 1000
""")

for row in cursor:
    oid = row[0]
    report = row[1]
    ugc = UGC(row[2][:2], row[2][2], row[2][3:])
    
    prod = product.TextProduct(report)
    
    for segment in prod.segments:
        if ugc not in segment.ugcs:
            continue
        #print ugc, segment.vtec[0].endts, prod.valid
        cursor2.execute("""UPDATE """+table+""" SET product_issue = %s,
    init_expire = %s WHERE oid = %s""", 
    (prod.valid, segment.vtec[0].endts, oid))

print 'Processed %s rows' % (cursor.rowcount,)    

cursor2.close()
POSTGIS.commit()
POSTGIS.close()