import pg, sys, re
mydb = pg.connect("afos", "iemdb")

data = sys.stdin.read().replace("\n", "z")

tokens = re.findall("(K[A-Z0-9]{3} [DM]S.*?[=N]z)", data)

for t in tokens:
  mydb.query("INSERT into products(pil, data) values('%s%s','%s')" % \
   (sys.argv[1], t[1:4] , t.replace("z","\n") ) )