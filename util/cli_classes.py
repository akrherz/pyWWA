import psycopg2
import pytz
from pyiem.nws.products.cli import parser, REGIMES

pgconn = psycopg2.connect(database='afos', host='iemdb', user='nobody')
cursor = pgconn.cursor()

cursor.execute("""
 SELECT data, source, pil, entered from products_2015_0106 
 WHERE entered > '2015-01-04' and substr(pil,1,3) = 'CLI' ORDER by entered asc
""")

o = open('cli.csv', 'w')
for row in cursor:
    ts = row[3].astimezone(pytz.timezone("UTC"))
    try:
        p = parser(row[0])
    except:
        print 'FAIL', row[1], row[2]
    
    if p.regime is not None and p.regime != 0:
        o.write("%s,%s,%s,%s,%s\n" % (ts.strftime("%Y-%m-%d %H:%M"), row[1], 
                row[2], p.regime, REGIMES[p.regime]))
    
o.close()