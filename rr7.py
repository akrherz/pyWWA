import pg, sys, re
mydb = pg.connect("afos", "iemdb")

data = sys.stdin.read().replace("\n", "z")

tokens = re.findall("(\.A [A-Z0-9]{3} .*?=)", data)

for t in tokens:
  mydb.query("INSERT into products(pil, data) values('%s%s','%s')" % \
   ('RR7', t[3:6] , t.replace("z","\n") ) )