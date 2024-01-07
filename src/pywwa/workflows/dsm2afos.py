"""Move DSM messages into the text database with the proper PIL."""
# Local
import re
import sys

from pyiem.nws import product

from pywwa.database import get_dbconnc


def main():
    """Go!"""
    pgconn, acursor = get_dbconnc("afos")

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
            f"{sys.argv[1]}{token[1:4]}",
            token.replace("z", "\n"),
            nws.source,
            nws.wmo,
            nws.valid.strftime("%Y-%m-%d %H:%M+00"),
        )
        acursor.execute(sql, sqlargs)

    acursor.close()
    pgconn.commit()
    pgconn.close()
