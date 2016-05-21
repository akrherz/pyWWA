# Send products from AFOS database to pyWWA

import psycopg2
AFOS = psycopg2.connect(database='afos', host='iemdb', port=5555, user='nobody')
acursor = AFOS.cursor()


o = open('FFW.txt', 'w')
acursor.execute("""with d as (SELECT data, entered from products_2015_0712
   WHERE substr(pil,1,3) = 'FFW' and entered > '2015-10-01')
   SELECT data from d WHERE data ~* 'FLASH FLOOD EMERGENCY'
    ORDER by entered""")
for row in acursor:
    # o.write('\001\r\r\n')
    o.write(row[0])
    o.write('\r\r\n\003')
o.close()
