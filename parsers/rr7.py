"""
"""
import sys
import re
import pytz
import datetime
import psycopg2

AFOS = psycopg2.connect(database="afos", host='iemdb')


acursor = AFOS.cursor()

data = sys.stdin.read().replace("\r\r\n", "z")

tokens = re.findall("(\.A [A-Z0-9]{3} .*?=)", data)

utcnow = datetime.datetime.utcnow()
gmt = utcnow.replace(tzinfo=pytz.timezone("UTC"))
gmt = gmt.replace(second=0)

table = "products_%s_0106" % (gmt.year,)
if gmt.month > 6:
    table = "products_%s_0712" % (gmt.year,)

for t in tokens:
    sql = """INSERT into """ + table + """
    (pil, data, entered) values(%s,%s,%s)"""
    sqlargs = ("%s%s" % ('RR7', t[3:6]), t.replace("z", "\n"), gmt)
    acursor.execute(sql, sqlargs)

acursor.close()
AFOS.commit()
AFOS.close()
