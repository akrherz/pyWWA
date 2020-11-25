""" Unidata LDM pqact bridge."""

# 3rd Party
from pyiem.util import LOG
from pyldm import ldmbridge

# Local
from pywwa.common import shutdown, email_error, SETTINGS


class MyProductIngestor(ldmbridge.LDMProductReceiver):
    """ I receive products from ldmbridge and process them 1 by 1 :) """

    def __init__(self, callback, isbinary=False):
        """ Constructor."""
        dedup = SETTINGS.get("pywwa_dedup", "false").lower() == "true"
        super().__init__(isbinary=isbinary, dedup=dedup)
        self.local_callback = callback

    def connectionLost(self, reason):
        """ called when the connection is lost """
        shutdown()

    def process_data(self, data):
        """ Override below."""
        self.local_callback(data)


def bridge(callback, dbpool=None, isbinary=False, product_end=None):
    """Build a pqact bridge.

    Params:
      callback (func): The callback function.
      dbpool (adapi): Database pool to use for callback with transaction.
      isbinary (bool): Is LDM processing binary/bytes vs strings.
      product_end (bytes): A custom product delimiter.
    """

    def proxy(data):
        """standardized transaction callback."""
        defer = dbpool.runInteraction(callback, data)
        defer.addErrback(email_error, data)
        defer.addErrback(LOG.error)

    proto = MyProductIngestor(
        callback if dbpool is None else proxy, isbinary=isbinary
    )
    if product_end is not None:
        proto.product_end = product_end
    return ldmbridge.LDMProductFactory(proto)
