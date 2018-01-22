"""Assign ETNs for what should have been the proper ETN

See: https://mesonet.agron.iastate.edu/onsite/news.phtml?id=1366

"""
from __future__ import print_function
from pandas.io.sql import read_sql
from pyiem.util import get_dbconn


def workflow(cursor, wfo, phenomena, significance):
    """Attempt to find dups"""
    # 1. Get the max eventid, which will we use to 'create' new ones
    cursor.execute("""
        SELECT max(eventid) from warnings_2017 where
        wfo = %s and phenomena = %s and significance = %s
    """, (wfo, phenomena, significance))
    maxetn = cursor.fetchone()[0]
    newetn = maxetn + 1
    firsthit = True
    # 2. Gap analysis
    for etn in range(1, maxetn + 1):
        cursor.execute("""
        with data as (
            select
            distinct generate_series(issue, expire, '1 minute'::interval)
            as ts from warnings_2017 where wfo = %s and phenomena = %s
            and significance = %s and eventid = %s ORDER by ts),
        agg as (
            SELECT ts, ts - lag(ts) OVER (ORDER by ts ASC) as diff from data)
        SELECT * from agg where diff > '1 day'::interval
        """, (wfo, phenomena, significance, etn))
        if cursor.rowcount > 1:
            print("  FIXME! %s %s %s %s" % (wfo, phenomena, significance, etn))
            continue
        elif cursor.rowcount == 0:
            continue
        if firsthit and etn != 1:
            print("would skip this!")
            continue
        row = cursor.fetchone()
        cursor.execute("""
        UPDATE warnings_2017 SET eventid = %s WHERE
        wfo = %s and phenomena = %s and significance = %s and eventid = %s
        and issue >= %s
        """, (newetn, wfo, phenomena, significance, etn, row[0]))
        print(("update %s %s %s %s -> %s %s %s"
               ) % (wfo, phenomena, significance, etn, newetn, row[0],
                    cursor.rowcount))
        newetn += 1
        firsthit = False


def main():
    """Go Main Go"""
    pgconn = get_dbconn('postgis')
    df = read_sql("""
    SELECT distinct wfo, phenomena, significance from warnings_2017
    ORDER by wfo ASC
    """, pgconn, index_col=None)
    for _, row in df.iterrows():
        cursor = pgconn.cursor()
        cursor.execute("""SET TIME ZONE 'UTC'""")
        workflow(cursor, row['wfo'], row['phenomena'], row['significance'])
        cursor.close()
        pgconn.commit()


if __name__ == '__main__':
    main()
