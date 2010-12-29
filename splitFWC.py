import pg, sys, re, string
mydb = pg.connect("afos", "iemdb")

d = string.strip(sys.stdin.read())

offset = string.find(d, "KWNO") + 12

sections = re.split("\n\n", d[offset:])
#print sections
#print len(sections)

for sect in sections:
        #print sect[1:4]
        mydb.query("INSERT into products(pil, data) values('%s','%s')" % \
                ("FWC" + sect[:3], d[:offset] + sect) )