"""Send products from AFOS database to pyWWA"""
import sys

import psycopg2
from pyiem.util import noaaport_text


def main(argv):
    """Go Main"""
    pgconn = psycopg2.connect(database='afos', host='iemdb', port=5555,
                              user='nobody')
    acursor = pgconn.cursor()

    pil = argv[1]

    output = open('%s.txt' % (pil, ), 'a')
    acursor.execute("""
        SELECT data, entered from products
        WHERE pil = %s
        ORDER by entered ASC""", (pil, ))
    for row in acursor:
        output.write(noaaport_text(row[0]))
    output.close()


if __name__ == '__main__':
    main(sys.argv)
