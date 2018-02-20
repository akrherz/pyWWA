"""Where to start...

Hack the database to convert stored warnings with counties to zones, when
there is one 2 one relationship here.  This likely breaks other things, but
alas

"""
from __future__ import print_function
from pyiem.util import get_dbconn
pgconn = get_dbconn('postgis')
cursor = pgconn.cursor()
cursor2 = pgconn.cursor()

cursor.execute("""
    WITH one as (
        SELECT ugc, name from ugcs where wfo = 'EWX' and end_ts is null
        and substr(ugc, 3, 1) = 'Z'),
    two as (
        SELECT ugc, name from ugcs where wfo = 'EWX' and end_ts is null
        and substr(ugc, 3, 1) = 'C')

    SELECT one.ugc, two.ugc from one JOIN two on (one.name = two.name)""")

xref = {'TXC123': 'TXZ224'}
for row in cursor:
    xref[row[1]] = row[0]

cursor.execute("""SELECT ugc, oid, issue from warnings WHERE wfo = 'EWX' and
 phenomena = 'FF' and significance = 'A' and substr(ugc, 3, 1) = 'C'""")
for row in cursor:
    oid = row[1]
    ugc = row[0]
    table = "warnings_%s" % (row[2].year,)
    cursor2.execute("""UPDATE """+table+""" SET ugc = %s,
    gid = get_gid(%s,issue) WHERE oid = %s""", (xref[ugc], xref[ugc], oid))
    print("%s %s %s %s" % (cursor2.rowcount, row[2], ugc, xref[ugc]))

cursor2.close()
pgconn.commit()
