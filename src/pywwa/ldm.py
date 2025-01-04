"""Unidata LDM pqact bridge."""

# 3rd Party
from twisted.enterprise.adbapi import ConnectionPool
from twisted.internet import reactor, task

# Local
from pywwa import LOG, ldmbridge
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
        LOG.info("STDIN Closed: %s, reactor.stop() in 1 second.", reason)
        reactor.callLater(1, reactor.stop)

    def process_data(self, data):
        """Override below."""
        self.local_callback(data)


def bridge(
    callback,
    dbpool: ConnectionPool = None,
    isbinary=False,
    product_end=None,
    cb2=None,
):
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
        if cb2 is not None:
            defer.addCallback(cb2)
        defer.addErrback(email_error, data)
        defer.addErrback(LOG.error)

    def nodbproxy(data):
        """Just do a deferred."""
        defer = task.deferLater(reactor, 0, callback, data)
        if cb2 is not None:
            defer.addCallback(cb2)
        defer.addErrback(email_error, data)
        defer.addErrback(LOG.error)

    proto = MyProductIngestor(
        nodbproxy if dbpool is None else dbproxy,
        isbinary=isbinary,
        product_end=product_end,
    )

    # If we have a dbpool, we don't want the reactor to stop until it clears
    # out the queue
    if dbpool is not None:

        def _connectionLost(_reason=None):
            """Shutdown the reactor if we are done."""
            waiting = dbpool.threadpool._queue.qsize()
            LOG.info(
                "STDIN closed. dbpool[dbname=%s] waiting: %s",
                dbpool.connkw.get("dbname"),
                waiting,
            )
            if waiting == 0:
                reactor.callLater(1, reactor.stop)
            else:
                reactor.callLater(10, _connectionLost, _reason)

        proto.connectionLost = _connectionLost

    ldmbridge.LDMProductFactory(proto)
    return proto
