"""Test pywwa.common"""

import pywwa
from pyiem.util import utc
from pywwa import common


def test_crawl():
    """Test crawling before walking."""
    assert common


def test_shutdown_badarg():
    """Test what happens when providing a bad argument to shutdown."""
    common.shutdown("5")


def test_shutdown():
    """Test shutdown."""
    common.shutdown()


def test_should_email():
    """Test that our logic to prevent email bombs works."""
    common.SETTINGS["pywwa_email_limit"] = 10
    for _ in range(30):
        common.EMAIL_TIMESTAMPS.append(utc())
    assert not common.should_email()
    assert not common.email_error(None, None)


def test_email_error():
    """Test that we can email an error."""
    common.email_error(None, None)
    pywwa.CTX.disable_email = True
    common.email_error(None, None)
