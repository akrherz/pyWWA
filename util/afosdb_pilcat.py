# Send products from AFOS database to pyWWA

import psycopg2
import sys
AFOS = psycopg2.connect(database='afos', host='iemdb', port=5555,
                        user='nobody')
acursor = AFOS.cursor()

PIL = sys.argv[1]

o = open('%s.txt' % (PIL, ), 'a')
acursor.execute("""
    SELECT data, entered from products_2017_0106
    WHERE pil = %s
    ORDER by entered LIMIT 2""", (PIL, ))
for row in acursor:
    # o.write('\001\r\r\n')
    o.write(row[0])
    o.write('\r\r\n\003')
o.close()
