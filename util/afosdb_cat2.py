# Send products from AFOS database to pyWWA

import psycopg2
import os
AFOS = psycopg2.connect(database='afos', host='iemdb', port=5555,
                        user='nobody')
acursor = AFOS.cursor('streamer')

o = open('KUNR.txt', 'w')

acursor.execute("""
    SELECT pil, entered at time zone 'UTC', source, pil from products
    WHERE source = 'KUNR'""")
for i, row in enumerate(acursor):
    uri = "http://mesonet.agron.iastate.edu/wx/afos/p.php?pil=%s&e=%s" % (
                    row[0], row[1].strftime("%Y%m%d%H%M"))
    o.write("%s,%s,%s,%s\n" % (row[1].strftime("%Y-%m-%d %H:%M"),
                               row[1].hour, row[0], uri))
    # skip below
    continue
    mydir = "KOKX/%s" % (row[3],)
    if not os.path.isdir(mydir):
        os.makedirs(mydir)
    o = open('%s/%s_%s_%s.txt' % (mydir, row[1].strftime("%Y%m%d%H%M"),
                                  row[2], row[3]),
             'a')
    o.write(row[0])
    o.write('\r\r\n\003')
    o.close()

o.close()
