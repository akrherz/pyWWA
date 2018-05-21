"""
Split the MAV product into bitesized chunks that the AFOS viewer can see
"""
import re
import sys

from pyiem.util import get_dbconn
from pyiem.nws import product


def main():
    """Go!"""
    pgconn = get_dbconn('afos')
    cursor = pgconn.cursor()

    payload = getattr(sys.stdin, 'buffer', sys.stdin).read()
    prod = product.TextProduct(payload.decode('ascii', errors='ignore'))
    prod.valid = prod.valid.replace(second=0, minute=0, microsecond=0)
    offset = prod.unixtext.find(prod.afos[:3]) + 7
    sections = re.split("\n\n", prod.unixtext[offset:])

    table = "products_%s_0106" % (prod.valid.year,)
    if prod.valid.month > 6:
        table = "products_%s_0712" % (prod.valid.year,)

    for sect in sections:
        if sect[1:4].strip() == "":
            continue
        # print("%s%s %s %s %s" % (prod.afos[:3], sect[1:4], prod.source,
        #                          prod.valid, prod.wmo))
        cursor.execute("""
            INSERT into """+table+"""
            (pil, data, source, entered, wmo) values (%s, %s, %s, %s, %s)
        """, (prod.afos[:3] + sect[1:4], prod.text[:offset] + sect,
              prod.source, prod.valid, prod.wmo))

    cursor.close()
    pgconn.commit()
    pgconn.close()


if __name__ == '__main__':
    main()
