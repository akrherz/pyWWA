"""Split RR7 products into their individual stations, so for AFOS discovery.

It is unclear why this is being done, but will keep it going for now.  These
products are culled after 10 days per AFOS database cleaning and that this is
SHEF data.
"""

# stdlib
import re

import click
from pyiem.nws.product import TextProduct

from pywwa import common
from pywwa.database import get_database
from pywwa.ldm import bridge


def real_process(txn, data):
    """Process the data"""
    prod = TextProduct(
        data, parse_segments=False, utcnow=common.utcnow(), ugc_provider={}
    )

    data = data.replace("\r\r\n", "z")

    tokens = re.findall(r"(\.A [A-Z0-9]{3} .*?=)", data)

    for token in tokens:
        sql = (
            "INSERT into products (pil, data, entered, source, wmo, bbb) "
            "values(%s, %s, %s, %s, %s, %s)"
        )
        sqlargs = (
            f"RR7{token[3:6]}",
            token.replace("z", "\n"),
            prod.valid,
            prod.source,
            prod.wmo,
            prod.bbb,
        )
        txn.execute(sql, sqlargs)


@click.command(help=__doc__)
@common.init
@common.disable_xmpp
def main(*args, **kwargs):
    """Go"""
    bridge(real_process, dbpool=get_database("afos"))
