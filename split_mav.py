"""
Split the MAV product into bitesized chunks that the AFOS viewer can see
"""
import pg, sys, re, string
mydb = pg.connect("afos", "iemdb")

d = string.strip(sys.stdin.read())

offset = string.find(d, sys.argv[1]) + 7

sections = re.split("\n\n", d[offset:])
#print sections
#print len(sections)

for sect in sections:
        #print sys.argv[1][:3] + sect[1:4]
        mydb.query("INSERT into products(pil, data) values('%s','%s')" % \
                (sys.argv[1][:3] + sect[1:4], d[:offset] + sect) )
