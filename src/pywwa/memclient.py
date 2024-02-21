"""A memcache client that is meant to be run from a thread within twisted."""
import threading

from pymemcache.client import Client


def write_memcache(key, value, expire=600):
    """Write a key/value pair to memcache"""

    def _write():
        """Do the actual write, from a thread."""
        # NB 1 second was too tight
        mc = Client(("iem-memcached", 11211), connect_timeout=6)
        mc.set(key, value, expire=expire)
        mc.close()

    threading.Thread(target=_write).start()
