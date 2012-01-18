# Send products from AFOS database to pyWWA

import iemdb
AFOS = iemdb.connect('afos', bypass=True)
acursor = AFOS.cursor()

acursor.execute("""SELECT data from products_2011_0712 WHERE substr(pil,1,3)  = 'MET'
and entered > '2011-11-16 01:00' and entered < '2011-11-16 12:00'
    ORDER by entered""")
for row in acursor:
    print row[0]
    print '\003'
