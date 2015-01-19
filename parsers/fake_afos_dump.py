"""
  Dump some stuff without AFOS PILs
"""

import psycopg2, sys
from pyiem.nws.product import TextProduct
AFOS = psycopg2.connect(database='afos', host='iemdb')
acursor = AFOS.cursor()

def main():
    ''' We are called with a hard coded AFOS PIL '''
    pil = sys.argv[1]
    tp = TextProduct( sys.stdin.read() )
    utc = tp.valid
    table = "products_%s_0106" % (utc.year,)
    if utc.month > 6:
        table = "products_%s_0712" % (utc.year,)
        
    sql = """INSERT into """+table+"""(pil, data, source, wmo, entered) 
        values(%s,%s,%s,%s,%s)"""
    
    sqlargs = (pil, tp.text,
               tp.source, tp.wmo, utc.strftime("%Y-%m-%d %H:%M+00") )
    acursor.execute(sql, sqlargs)

if __name__ == '__main__':
    main()
    acursor.close()
    AFOS.commit()
    AFOS.close()