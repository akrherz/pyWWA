"""
Chunk the MOS text data into easier to search values.
"""
import re

from twisted.internet import reactor
from twisted.python import log
from pyldm import ldmbridge
from pyiem.nws import product
import common  # @UnresolvedImport

DBPOOL = common.get_database("afos")


class MyProductIngestor(ldmbridge.LDMProductReceiver):
    """Do what we do!"""

    prods = 0

    def process_data(self, data):
        """
        Actual ingestor
        """
        self.prods += 1
        defer = DBPOOL.runInteraction(real_process, data)
        defer.addErrback(common.email_error, data)

    def connectionLost(self, reason):
        """
        Called when ldm closes the pipe
        """
        log.msg("processed %s prods" % (self.prods,))
        reactor.callLater(5, shutdown)


def shutdown():
    """Shutme off"""
    reactor.callWhenRunning(reactor.stop)


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
    raw = raw.replace("\n", "___").replace("\x1e", "")
    sections = re.findall(r"([A-Z0-9]{4}\s+... ... GUIDANCE .*?)______", raw)

    table = "products_%s_0106" % (prod.valid.year,)
    if prod.valid.month > 6:
        table = "products_%s_0712" % (prod.valid.year,)

    for sect in sections:
        # print("%s%s %s %s %s" % (prod.afos[:3], sect[1:4], prod.source,
        #                          prod.valid, prod.wmo))
        txn.execute(
            """
            INSERT into """
            + table
            + """
            (pil, data, source, entered, wmo) values (%s, %s, %s, %s, %s)
        """,
            (
                prod.afos[:3] + sect[1:4],
                header + sect.replace("___", "\n"),
                prod.source,
                prod.valid,
                prod.wmo,
            ),
        )


if __name__ == "__main__":
    ldmbridge.LDMProductFactory(MyProductIngestor())
    reactor.run()
