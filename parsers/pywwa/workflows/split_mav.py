"""
Chunk the MOS text data into easier to search values.
"""
# stdlib
import re

# 3rd Party
from twisted.internet import reactor
from pyiem.nws import product

# Local
from pywwa.ldm import bridge
from pywwa.database import get_database


def real_process(txn, data):
    """Go!"""
    prod = product.TextProduct(data)
    # replicate functionality in pyiem/nws/products/mos.py
    header = "000 \n%s %s %s\n%s\n" % (
        prod.wmo,
        prod.source,
        prod.valid.strftime("%d%H%M"),
        prod.afos,
    )
    raw = prod.unixtext + "\n"
    # Since we only do realtime processing, this is OK, I hope
    sections = raw.split("\x1e")

    for sect in sections:
        tokens = re.findall(
            r"(^[A-Z0-9_]{3,10}\s+....? V?[0-9]?\.?[0-9]?\s?....? GUIDANCE)",
            sect,
        )
        if not tokens:
            # log.msg("Nothing found in section of len=%s" % (len(sect), ))
            continue
        # Only take 4 char IDs :/
        if len(sect[:100].split()[0]) != 4:
            continue
        # print("%s%s %s %s %s" % (prod.afos[:3], sect[1:4], prod.source,
        #                          prod.valid, prod.wmo))
        txn.execute(
            """
            INSERT into products
            (pil, data, source, entered, wmo) values (%s, %s, %s, %s, %s)
        """,
            (
                prod.afos[:3] + sect[1:4],
                header + sect.replace(";;;", "\n"),
                prod.source,
                prod.valid,
                prod.wmo,
            ),
        )


def main():
    """Go Main Go."""
    bridge(real_process, dbpool=get_database("afos"))
    reactor.run()


if __name__ == "__main__":
    main()
