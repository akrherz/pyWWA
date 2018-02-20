"""Assign faked VTEC eventids for archived products prior to VTEC"""
from __future__ import print_function
import os
import sys
import cPickle
from pyiem.util import get_dbconn

pgconn = get_dbconn('postgis')
cursor = pgconn.cursor()
cursor2 = pgconn.cursor()

yr = sys.argv[1]

touches = {}
if os.path.isfile('touches.pickle'):
    touches = cPickle.load(open("touches.pickle", 'rb'))

table = "warnings_%s" % (yr, )


def load_touches(ugc, wfo):
    c = pgconn.cursor()
    c.execute("""SELECT ugc from ugcs where
    ST_Touches(geom, (SELECT geom from ugcs where ugc = %s and end_ts is null))
    and ugc != %s and wfo = %s and substr(ugc, 3, 1) = %s and end_ts is null
    """, (ugc, ugc, wfo, ugc[2]))
    for row in c:
        a = touches.setdefault(ugc, [])
        a.append(row[0])


cursor.execute("""
  SELECT oid, issue, expire, ugc, wfo, phenomena, significance
  from """ + table + """ WHERE
  eventid is null ORDER by issue ASC
  """)

for row in cursor:
    (oid, issue, expire, ugc, wfo, phenomena, significance) = row
    if ugc not in touches:
        load_touches(ugc, wfo)
        f = open('touches.pickle', 'wb')
        cPickle.dump(touches, f, 2)
        f.close()

    print("%s %s" % (ugc, touches.get(ugc)))
    if touches.get(ugc) is not None:
        # OK, look for other entries that match this one
        cursor2.execute("""SELECT eventid, ugc from """ + table + """
        WHERE issue = %s and expire = %s and phenomena = %s and
        significance = %s and wfo = %s and eventid is not null
        and ugc in %s
        """, (issue, expire, phenomena, significance, wfo,
              tuple(touches.get(ugc, []))))
        if cursor2.rowcount > 0:
            row = cursor2.fetchone()
            print('WINNER? %s' % (row[0], ))
            cursor2.execute("""UPDATE """ + table + """ SET eventid = %s
            WHERE oid = %s""", (row[0], oid))
            continue

    # Can we sequence this warning?
    cursor2.execute("""SELECT eventid, min(issue) from """ + table + """
    WHERE phenomena = %s and significance = %s and wfo = %s
    and eventid is not null GROUP by eventid ORDER by min ASC
    """, (phenomena, significance, wfo))
    if cursor2.rowcount == 0:
        print('Assign 1? %s %s %s %s' % (wfo, phenomena, significance, ugc))
        cursor2.execute("""UPDATE """ + table + """ SET
        eventid = 1 WHERE oid = %s """, (oid, ))
        continue
    events = []
    issues = []
    for row in cursor2:
        events.append(row[0])
        issues.append(row[1])
    if issue >= issues[-1]:
        print("INCREMENT! %s %s" % (issue, events[-1] + 1))
        cursor2.execute("""UPDATE """ + table + """ SET eventid = %s
        WHERE oid = %s""", (events[-1] + 1, oid))
        continue
    if issue < issues[0]:
        print('Uh oh %s %s %s' % (events[0], issue, issues[0]))
        cursor2.execute("""UPDATE """ + table + """ SET eventid = %s
        WHERE oid = %s""", (events[-1] + 1, oid))
    for i in range(len(events)-1):
        if issues[i] < issue and issues[i+1] > issue:
            if events[i+1] < events[i]:
                print('BACKWARDS %s %s' % (events[i], events[i+1]))
                cursor2.execute("""UPDATE """ + table + """ SET eventid = %s
                WHERE oid = %s""", (events[i] + 1, oid))
                continue
            print('HERE %s %s %s %s %s' % (oid, issue, issues[i], issues[i+1], events[i]))
            continue

    # see if this is a DUP!
    cursor2.execute("""SELECT count(*) from """ + table + """ WHERE
    oid != %s and wfo = %s and ugc = %s and phenomena = %s
    and significance = %s
    and issue = %s""", (oid, wfo, ugc, phenomena, significance, issue))
    if cursor2.fetchone()[0] > 0:
        print('DUP! %s %s %s %s' % (oid, wfo, phenomena, issue))
        cursor2.execute("""DELETE from """ + table + """ WHERE
        oid = %s""", (oid, ))
        continue
    print('Fail? %s %s %s %s %s %s' % (oid, issue, issues[0], events[0], issues[-1], events[-1]))
    continue

cursor.close()
cursor2.close()
pgconn.commit()
pgconn.close()
