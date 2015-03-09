"""
Move DSM messages into the text database with the proper PIL
"""
import psycopg2
import sys
import re
from pyiem.nws import product
AFOS = psycopg2.connect(database='afos', host='iemdb')
acursor = AFOS.cursor()

raw = sys.stdin.read()
data = raw.replace("\r\r\n", "z")
tokens = re.findall("(K[A-Z0-9]{3} [DM]S.*?[=N]z)", data)

nws = product.TextProduct(raw)

gmt = nws.valid
table = "products_%s_0106" % (gmt.year,)
if gmt.month > 6:
    table = "products_%s_0712" % (gmt.year,)

sql = """INSERT into """+table+"""(pil, data, source, wmo, entered)
    values(%s,%s,%s,%s,%s)"""

for t in tokens:
    sqlargs = ("%s%s" % (sys.argv[1], t[1:4]), t.replace("z", "\n"),
               nws.source, nws.wmo, gmt.strftime("%Y-%m-%d %H:%M+00"))
    acursor.execute(sql, sqlargs)

acursor.close()
AFOS.commit()
AFOS.close()
