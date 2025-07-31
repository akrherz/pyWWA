"""Test the ldmbrige."""

from datetime import datetime, timezone
from unittest import mock

import pytest

from pywwa import ldmbridge


def test_process_data():
    """Test process_data."""
    proto = ldmbridge.LDMProductReceiver(dedup=True)
    with pytest.raises(NotImplementedError):
        proto.process_data("test\r\r\ntest\r\r\n")


def test_lineReceived():
    """Test lineReceived."""
    proto = ldmbridge.LDMProductReceiver(dedup=True)
    proto.reactor = mock.Mock()
    proto.process_data = mock.Mock()
    proto.cache["test"] = datetime.now(timezone.utc)
    proto.cache["test2"] = datetime(2007, 1, 1, 12, 14, tzinfo=timezone.utc)
    proto.clean_cache()
    proto.filter_product(r"test\r\r\ntest\r\r\ntest\x17\r\r\n\r\r\n")
    proto.filter_product(r"\r\r\n\r\r\n\r\r\n")
    proto.filter_product(r"test\r\r\ntest\r\r\ntest\x17\r\r\n\r\r\n")

    proto.rawDataReceived(
        b"\001000 \r\r\nSXUS50 KISU 010000\r\r\nADMA\r\r\n\003"
    )
    proto.lineReceived(b"000 \r\r\nSXUS50 KISU 010000\r\r\nADMA\r\r\n\003")
