"""A memcache client that is meant to be used from within twisted.

Writes are queued and processed by a single long-lived daemon thread that
holds a persistent connection, so that bursts of products do not spawn a
thread and a TCP connection per write.
"""

import logging
import queue
import threading

from pymemcache.client import Client

LOG = logging.getLogger(__name__)
MEMCACHE_ADDR = ("iem-memcached-cloud", 11211)
_QUEUE: queue.Queue = queue.Queue(maxsize=1000)
_LOCK = threading.Lock()
_THREAD: threading.Thread | None = None


def _process_one(mc, item):
    """Write one item, returning the client to keep (None to reconnect)."""
    key, value, expire = item
    try:
        if mc is None:
            # NB 1 second was too tight
            mc = Client(MEMCACHE_ADDR, connect_timeout=10, timeout=10)
        mc.set(key, value, expire=expire)
    except Exception as exp:
        LOG.warning("memcache write of %s failed: %s", key, exp)
        if mc is not None:
            try:
                mc.close()
            except Exception as close_exp:
                LOG.warning(
                    "memcache close after write failure for %s failed: %s",
                    key,
                    close_exp,
                )
        return None
    return mc


def _worker():
    """Drain the queue, reconnecting as necessary."""
    mc = None
    while True:
        mc = _process_one(mc, _QUEUE.get())


def _ensure_worker():
    """Start the worker thread if it is not running."""
    global _THREAD  # noqa: PLW0603
    with _LOCK:
        if _THREAD is None or not _THREAD.is_alive():
            _THREAD = threading.Thread(
                target=_worker, name="memclient", daemon=True
            )
            _THREAD.start()


def write_memcache(key, value, expire=600):
    """Queue a key/value pair to be written to memcache."""
    _ensure_worker()
    try:
        _QUEUE.put_nowait((key, value, expire))
    except queue.Full:
        LOG.warning("memcache queue full, dropping %s", key)
