"""
Split the MAV product into bitesized chunks that the AFOS viewer can see
"""
import sys
import re
import datetime
import psycopg2
AFOS = psycopg2.connect(database='afos', host='iemdb')
cursor = AFOS.cursor()

d = sys.stdin.read().strip()

offset = d.find(sys.argv[1]) + 7

sections = re.split("\n\n", d[offset:])

utc = datetime.datetime.utcnow()
table = "products_%s_0106" % (utc.year,)
if utc.month > 6:
    table = "products_%s_0712" % (utc.year,)

for sect in sections:
    cursor.execute("""INSERT into """+table+"""(pil, data, source) 
        values(%s, %s, %s)""", 
        (sys.argv[1][:3] + sect[1:4], d[:offset] + sect, sect[:4] ))

cursor.close()
AFOS.commit()
AFOS.close()