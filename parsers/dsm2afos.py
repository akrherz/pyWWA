"""Move DSM messages into the text database with the proper PIL."""
# Local
import sys
import re

# 3rd Party
from pyiem.util import get_dbconn
from pyiem.nws import product


def main():
    """Go!"""
    pgconn = get_dbconn("afos")
    acursor = pgconn.cursor()

    raw = sys.stdin.read()
    data = raw.replace("\r\r\n", "z")
    tokens = re.findall("(K[A-Z0-9]{3} [DM]S.*?[=N]z)", data)

    nws = product.TextProduct(raw)

    sql = (
        "INSERT into products (pil, data, source, wmo, entered) "
        "values(%s,%s,%s,%s,%s) "
    )
    for token in tokens:
        sqlargs = (
            "%s%s" % (sys.argv[1], token[1:4]),
            token.replace("z", "\n"),
            nws.source,
            nws.wmo,
            nws.valid.strftime("%Y-%m-%d %H:%M+00"),
        )
        acursor.execute(sql, sqlargs)

    acursor.close()
    pgconn.commit()
    pgconn.close()


if __name__ == "__main__":
    main()
