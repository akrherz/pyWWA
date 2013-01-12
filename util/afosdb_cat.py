# Send products from AFOS database to pyWWA

import iemdb
AFOS = iemdb.connect('afos', bypass=True)
acursor = AFOS.cursor()

for pil in ['MWW','FWW','CFW','TCV','RFW','FFA','SVR','TOR','SVS',
    'SMW','MWS','NPW','WCN','WSW','EWW','FLS','FLW','FFW','FFS','HLS','TSU']:
    print pil
    o = open('all/%s.txt' % (pil,), 'w')
    acursor.execute("""SELECT data from products WHERE 
    substr(pil,1,3) = %s
    ORDER by entered""", (pil,))
    for row in acursor:
        o.write('\001\r\r\n')
        o.write(row[0])
        o.write('\r\r\n\003')
    o.close()
