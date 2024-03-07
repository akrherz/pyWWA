"""Move DSM messages into the text database with the proper PIL."""

# Local
import re

import click
from pyiem.nws import product

from pywwa import common
from pywwa.database import get_dbconnc
from pywwa.ldm import bridge


def workflow(raw):
    """Go!"""
    pgconn, acursor = get_dbconnc("afos")

    data = raw.replace("\r\r\n", "z")
    tokens = re.findall("(K[A-Z0-9]{3} [DM]S.*?[=N]z)", data)

    nws = product.TextProduct(raw)

    sql = (
        "INSERT into products (pil, data, source, wmo, entered) "
        "values(%s,%s,%s,%s,%s) "
    )
    pil3 = "DSM" if nws.wmo == "CDUS27" else "MSM"
    for token in tokens:
        sqlargs = (
            f"{pil3}{token[1:4]}",
            token.replace("z", "\n"),
            nws.source,
            nws.wmo,
            nws.valid.strftime("%Y-%m-%d %H:%M+00"),
        )
        acursor.execute(sql, sqlargs)

    acursor.close()
    pgconn.commit()
    pgconn.close()
    return nws


@click.command(help=__doc__)
@common.init
@common.disable_xmpp
def main(*args, **kwargs):
    """Go Main Go."""
    bridge(workflow)
