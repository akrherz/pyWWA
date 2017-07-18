"""Check METAR ingest"""
from __future__ import print_function

import psycopg2

SQL = """
        SELECT c.iemid, t.id,  tmpf, raw, valid
        from current_log c JOIN stations t
        on (c.iemid = t.iemid) WHERE t.network ~* 'ASOS'
        and c.valid > now() - '1 hour'::interval
        and raw !~* 'MADIS'
"""


def main():
    """Go Main!"""
    pgconn = psycopg2.connect(database='iem')
    laptop = {}
    cursor = pgconn.cursor()
    cursor.execute(SQL)
    for row in cursor:
        laptop[row[1]] = row

    pgconn = psycopg2.connect(database='iem', host='localhost', port=5555,
                              user='nobody')
    cursor = pgconn.cursor()
    cursor.execute(SQL)
    for row in cursor:
        if row[1] not in laptop:
            continue
        lrow = laptop[row[1]]
        if row[4] > lrow[4]:
            print("ID: %s IEM valid: %s laptop: %s" % (row[1],
                                                       row[4],
                                                       lrow[4]))
        elif row[3] != lrow[3]:
            print("iemid: %s\nlaptop: '%s'\nIEM   : '%s'" % (row[1],
                                                             lrow[3],
                                                             row[3]))


if __name__ == '__main__':
    main()
