"""Split RR7 products, for some reason!
"""
import sys
import re
import datetime

import pytz
from pyiem.util import get_dbconn


def main():
    """Go"""
    pgconn = get_dbconn("afos")

    acursor = pgconn.cursor()

    payload = getattr(sys.stdin, "buffer", sys.stdin).read()
    payload = payload.decode("ascii", errors="ignore")
    data = payload.replace("\r\r\n", "z")

    tokens = re.findall(r"(\.A [A-Z0-9]{3} .*?=)", data)

    utcnow = datetime.datetime.utcnow()
    gmt = utcnow.replace(tzinfo=pytz.utc)
    gmt = gmt.replace(second=0)

    table = "products_%s_0106" % (gmt.year,)
    if gmt.month > 6:
        table = "products_%s_0712" % (gmt.year,)

    for token in tokens:
        # print(tokens)
        sql = (
            """
        INSERT into """
            + table
            + """
        (pil, data, entered) values(%s,%s,%s)
        """
        )
        sqlargs = ("%s%s" % ("RR7", token[3:6]), token.replace("z", "\n"), gmt)
        acursor.execute(sql, sqlargs)

    acursor.close()
    pgconn.commit()
    pgconn.close()


if __name__ == "__main__":
    main()
