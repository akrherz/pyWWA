"""Test pywwa.common"""

import click
from click.testing import CliRunner
from pyiem.util import utc
from pywwa import common


def test_setup_syslog():
    """API exercice."""
    common.pywwa.CTX["stdout_logging"] = True
    common.setup_syslog()


def test_parse_utcnow():
    """Test that we can parse utcnow."""
    assert common.parse_utcnow("2017-01-01T12:00").year == 2017
    assert common.parse_utcnow(None) is None


def test_init_decorator():
    """Test the init decorator."""

    @click.command()
    @common.init
    def test_func(*args, **kwargs):
        """Test function."""
        return True

    runner = CliRunner()
    result = runner.invoke(test_func, args=["-l"])
    assert result.exit_code == 0


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
    common.EMAIL_TIMESTAMPS = []
    common.email_error(None, None)
    common.pywwa.CTX["disable_email"] = True
    common.email_error(None, None)
