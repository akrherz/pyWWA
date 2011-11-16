"""
Move DSM messages into the text database with the proper PIL
$Id: $:
"""
import iemdb, sys, re
AFOS = iemdb.connect('afos')
acursor = AFOS.cursor()

data = sys.stdin.read().replace("\n", "z")
tokens = re.findall("(K[A-Z0-9]{3} [DM]S.*?[=N]z)", data)

for t in tokens:
    sql = "INSERT into products(pil, data) values(%s,%s)"""
    sqlargs = ("%s%s" % (sys.argv[1], t[1:4]) , t.replace("z","\n") )
    acursor.execute(sql, sqlargs)
    
acursor.close()
AFOS.commit()
AFOS.close()