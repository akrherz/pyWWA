""" Unidata LDM pqact bridge."""

# 3rd Party
from pyiem.util import LOG
from twisted.internet import reactor, task

# Local
from pywwa import ldmbridge, shutdown
from pywwa.common import SETTINGS, email_error


class MyProductIngestor(ldmbridge.LDMProductReceiver):
    """I receive products from ldmbridge and process them 1 by 1 :)"""

    def __init__(self, callback, isbinary=False, **kwargs):
        """Constructor."""
        dedup = SETTINGS.get("pywwa_dedup", "false").lower() == "true"
        super().__init__(isbinary=isbinary, dedup=dedup, **kwargs)
        self.local_callback = callback

    def connectionLost(self, reason=None):
        """called when the connection is lost"""
        LOG.info("connectionLost: %s", reason)
        shutdown()

    def process_data(self, data):
        """Override below."""
        self.local_callback(data)


def bridge(callback, dbpool=None, isbinary=False, product_end=None, cb2=None):
    """Build a pqact bridge.

    Params:
      callback (func): The callback function.
      dbpool (adapi): Database pool to use for callback with transaction.
      isbinary (bool): Is LDM processing binary/bytes vs strings.
      product_end (bytes): A custom product delimiter.
    """

    def dbproxy(data):
        """standardized transaction callback."""
        defer = dbpool.runInteraction(callback, data)
        if cb2:
            defer.addCallback(cb2)
        defer.addErrback(email_error, data)
        defer.addErrback(LOG.error)

    def nodbproxy(data):
        """Just do a deferred."""
        defer = task.deferLater(reactor, 0, callback, data)
        if cb2:
            defer.addCallback(cb2)
        defer.addErrback(email_error, data)
        defer.addErrback(LOG.error)

    proto = MyProductIngestor(
        nodbproxy if dbpool is None else dbproxy,
        isbinary=isbinary,
        product_end=product_end,
    )
    ldmbridge.LDMProductFactory(proto)
    return proto
