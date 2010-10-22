# Send products from AFOS database to pyWWA

import iemdb
AFOS = iemdb.connect('afos', bypass=True)
acursor = AFOS.cursor()

acursor.execute("""SELECT data from products WHERE pil = 'PTSDY1'
    ORDER by entered ASC""")
for row in acursor:
    print row[0]
    print '\003'