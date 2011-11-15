# Send products from AFOS database to pyWWA

import iemdb
AFOS = iemdb.connect('afos', bypass=True)
acursor = AFOS.cursor()

acursor.execute("""SELECT data from products_2011_0712 WHERE pil = 'SVRDMX'
    ORDER by entered ASC""")
for row in acursor:
    print row[0]
    print '\003'
