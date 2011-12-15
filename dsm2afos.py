"""
Move DSM messages into the text database with the proper PIL
$Id: $:
"""
import iemdb, sys, re
from support import TextProduct
AFOS = iemdb.connect('afos')
acursor = AFOS.cursor()

raw = sys.stdin.read()
data = raw.replace("\n", "z")
tokens = re.findall("(K[A-Z0-9]{3} [DM]S.*?[=N]z)", data)

nws = TextProduct.TextProduct( raw, bypass=True)
nws.findAFOS()
nws.findWMO()

for t in tokens:
    sql = "INSERT into products(pil, data, source, wmo) values(%s,%s,%s,%s)"""
    sqlargs = ("%s%s" % (sys.argv[1], t[1:4]) , t.replace("z","\n"),
               nws.source, nws.wmo )
    acursor.execute(sql, sqlargs)
    
acursor.close()
AFOS.commit()
AFOS.close()