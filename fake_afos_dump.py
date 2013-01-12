"""
  Dump some stuff without AFOS PILs
"""

import iemdb, sys
AFOS = iemdb.connect('afos')
acursor = AFOS.cursor()

def main():
    pil = sys.argv[1]
    myData = sys.stdin.read()

    sql =  "INSERT into products (pil,data) VALUES (%s, %s)"
    sqlargs = (pil,myData)
    acursor.execute(sql, sqlargs)

if __name__ == '__main__':
    main()
    acursor.close()
    AFOS.commit()
    AFOS.close()