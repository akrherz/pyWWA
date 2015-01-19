# Send products from AFOS database to pyWWA

import psycopg2
AFOS = psycopg2.connect(database='afos', host='mesonet.agron.iastate.edu', user='nobody')
acursor = AFOS.cursor()


o = open('TORSVRSVS.txt' , 'w')
acursor.execute("""SELECT data from products_2014_0106 WHERE 
    substr(pil,1,3) in ('SVR','SVS','TOR') and entered > '2014-06-03 06:00'
    and entered < '2014-06-14 06:00'
    ORDER by entered""")
for row in acursor:
    #o.write('\001\r\r\n')
    o.write(row[0])
    o.write('\r\r\n\003')
o.close()
