"""
Chunk the MOS text data into easier to search values.
"""
# stdlib
import re

from pyiem.nws import product

# 3rd Party
from twisted.internet import reactor

# Local
from pywwa import common
from pywwa.database import get_database
from pywwa.ldm import bridge


def real_process(txn, data):
    """Go!"""
    prod = product.TextProduct(data)
    # replicate functionality in pyiem/nws/products/mos.py
    header = (
        "000 \n"
        f"{prod.wmo} {prod.source} {prod.valid:%d%H%M}\n"
        f"{prod.afos}\n"
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
        # The NWS cut some corners and ended up not using proper TTAAII values
        # for non US data.
        ttaaii = prod.wmo
        if sect[0] != "K" and ttaaii[2:4] == "US":
            ttaaii = f"{ttaaii[:2]}{sect[0]}{sect[0]}{ttaaii[4:6]}"
        txn.execute(
            "INSERT into products (pil, data, source, entered, wmo) "
            "values (%s, %s, %s, %s, %s)",
            (
                prod.afos[:3] + sect[1:4],
                header + sect.replace(";;;", "\n"),
                prod.source,
                prod.valid,
                ttaaii,
            ),
        )


def main():
    """Go Main Go."""
    common.main(with_jabber=False)
    bridge(real_process, dbpool=get_database("afos"))
    reactor.run()


if __name__ == "__main__":
    main()
