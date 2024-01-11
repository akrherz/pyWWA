"""Test pywwa.common"""

from pyiem.util import utc
from pywwa import CTX, common


def test_setup_syslog():
    """API exercice."""
    CTX["stdout_logging"] = True
    common.setup_syslog()


def test_parse_utcnow():
    """Test that we can parse utcnow."""
    assert common.parse_utcnow("2017-01-01T12:00").year == 2017
    assert common.parse_utcnow(None) is None


def test_crawl():
    """Test crawling before walking."""
    assert common


def test_should_email():
    """Test that our logic to prevent email bombs works."""
    common.SETTINGS["pywwa_email_limit"] = 10
    for _ in range(30):
        common.EMAIL_TIMESTAMPS.append(utc())
    assert not common.should_email()
    assert not common.email_error(None, None)


def test_email_error():
    """Test that we can email an error."""
    common.EMAIL_TIMESTAMPS = []
    common.email_error(None, None)
    CTX["disable_email"] = True
    common.email_error(None, None)
