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
    # need to dump non-ascii garbage
    prod = product.TextProduct(data.encode("ascii", "ignore").decode("ascii"))
    prod.valid = prod.valid.replace(second=0, minute=0, microsecond=0)
    offset = prod.unixtext.find(prod.afos[:3]) + 8
    sections = re.split("\n\n", prod.unixtext[offset:])

    table = "products_%s_0106" % (prod.valid.year,)
    if prod.valid.month > 6:
        table = "products_%s_0712" % (prod.valid.year,)

    for sect in sections:
        if sect[1:4].strip() == "":
            continue
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
                prod.unixtext[: offset - 1] + sect,
                prod.source,
                prod.valid,
                prod.wmo,
            ),
        )


if __name__ == "__main__":
    ldmbridge.LDMProductFactory(MyProductIngestor())
    reactor.run()
