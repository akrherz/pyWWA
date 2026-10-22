"""Test pywwa.memclient."""

import queue
from unittest import mock

from pywwa import memclient


def test_process_one_reuses_client():
    """Test a successful write keeps the client."""
    mc = mock.Mock()
    assert memclient._process_one(mc, ("k", "v", 5)) is mc
    mc.set.assert_called_once_with("k", "v", expire=5)


def test_process_one_failure_resets_client():
    """Test a failed write closes and drops the client."""
    mc = mock.Mock()
    mc.set.side_effect = TimeoutError("timed out")
    assert memclient._process_one(mc, ("k", "v", 5)) is None
    mc.close.assert_called_once()


def test_process_one_connect_failure():
    """Test a failed connect does not raise."""
    with mock.patch.object(memclient, "Client") as cl:
        cl.return_value.set.side_effect = TimeoutError("timed out")
        assert memclient._process_one(None, ("k", "v", 5)) is None


def test_write_memcache_queue_full():
    """Test a full queue drops rather than raising."""
    q = queue.Queue(maxsize=1)
    q.put(1)
    with (
        mock.patch.object(memclient, "_QUEUE", q),
        mock.patch.object(memclient, "_ensure_worker"),
    ):
        memclient.write_memcache("k", "v")
    assert q.qsize() == 1
