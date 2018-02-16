"""Send products from AFOS database to pyWWA"""

from pyiem.util import noaaport_text, get_dbconn
AFOS = get_dbconn('afos')
acursor = AFOS.cursor()


o = open('FFW.txt', 'w')
acursor.execute("""
    with d as (
        SELECT data, entered from products_2012_0106
        WHERE pil = 'SVSBMX' and entered > '2012-01-23' and entered < '2012-01-25')

    SELECT data from d ORDER by entered
""")
for row in acursor:
    o.write(noaaport_text(row[0]))
o.close()
