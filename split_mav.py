"""
Split the MAV product into bitesized chunks that the AFOS viewer can see
"""
import sys
import re
from pyiem.nws import product 
import psycopg2
AFOS = psycopg2.connect(database='afos', host='iemdb')
cursor = AFOS.cursor()

prod = product.TextProduct( sys.stdin.read() )
prod.valid = prod.valid.replace(second=0,minute=0,microsecond=0)
offset = prod.text.find(sys.argv[1]) + 7

sections = re.split("\n\n", prod.text[offset:])

table = "products_%s_0106" % (prod.valid.year,)
if prod.valid.month > 6:
    table = "products_%s_0712" % (prod.valid.year,)

for sect in sections:
    if sect[1:4].strip() == "":
        continue
    cursor.execute("""INSERT into """+table+""" 
        (pil, data, source, entered, wmo) 
        values(%s, %s, %s, %s, %s)""", 
        (sys.argv[1][:3] + sect[1:4], prod.text[:offset] + sect, 
         prod.source, prod.valid, prod.wmo ))

cursor.close()
AFOS.commit()
AFOS.close()