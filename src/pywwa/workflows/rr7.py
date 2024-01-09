"""Split RR7 products, for some reason!"""
# stdlib
import re

import click
from pyiem.util import utc

from pywwa import common
from pywwa.database import get_database
from pywwa.ldm import bridge


def real_process(txn, data):
    """Process the data"""
    data = data.replace("\r\r\n", "z")

    tokens = re.findall(r"(\.A [A-Z0-9]{3} .*?=)", data)

    utcnow = utc().replace(second=0, microsecond=0)

    for token in tokens:
        sql = "INSERT into products (pil, data, entered) values(%s,%s,%s)"
        sqlargs = (f"RR7{token[3:6]}", token.replace("z", "\n"), utcnow)
        txn.execute(sql, sqlargs)


@click.command()
@common.init
@common.disable_xmpp
def main(*args, **kwargs):
    """Go"""
    bridge(real_process, dbpool=get_database("afos"))
