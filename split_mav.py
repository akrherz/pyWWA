"""
Split the MAV product into bitesized chunks that the AFOS viewer can see
"""
import sys
import re
import string
import psycopg2
AFOS = psycopg2.connect(database='afos', host='iemdb')
cursor = AFOS.cursor()

d = string.strip(sys.stdin.read())

offset = string.find(d, sys.argv[1]) + 7

sections = re.split("\n\n", d[offset:])
#print sections
#print len(sections)

for sect in sections:
    #print sys.argv[1][:3] + sect[1:4]
    cursor.execute("""INSERT into products(pil, data, source) 
        values(%s, %s, %s)""", 
        (sys.argv[1][:3] + sect[1:4], d[:offset] + sect, sect[:4] ))

cursor.close()
AFOS.commit()
AFOS.close()