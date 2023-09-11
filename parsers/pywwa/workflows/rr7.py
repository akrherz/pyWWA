"""Split RR7 products, for some reason!"""
# stdlib
import re
import sys

# 3rd Party
from pyiem.util import get_dbconnc, utc


def main():
    """Go"""
    pgconn, acursor = get_dbconnc("afos")

    payload = getattr(sys.stdin, "buffer", sys.stdin).read()
    payload = payload.decode("ascii", errors="ignore")
    data = payload.replace("\r\r\n", "z")

    tokens = re.findall(r"(\.A [A-Z0-9]{3} .*?=)", data)

    utcnow = utc().replace(second=0, microsecond=0)

    for token in tokens:
        # print(tokens)
        sql = "INSERT into products (pil, data, entered) values(%s,%s,%s)"
        sqlargs = (f"RR7{token[3:6]}", token.replace("z", "\n"), utcnow)
        acursor.execute(sql, sqlargs)

    acursor.close()
    pgconn.commit()
    pgconn.close()


if __name__ == "__main__":
    main()
