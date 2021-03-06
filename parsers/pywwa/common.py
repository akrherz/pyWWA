"""Support lib for the parser scripts found in this directory"""
import os
import inspect
import logging
import pwd
import datetime
from io import StringIO
import socket
import sys
import traceback
from email.mime.text import MIMEText
from syslog import LOG_LOCAL2

# 3rd party
from twisted.python import log as tplog
from twisted.logger import formatEvent
from twisted.python import syslog
from twisted.python import failure
from twisted.internet import reactor
from twisted.mail import smtp

import pyiem
from pyiem.util import LOG, utc, CustomFormatter

# Local Be careful of circeref here
from pywwa import SETTINGS
from pywwa.cmdline import parse_cmdline
from pywwa.database import get_sync_dbconn

# http://bugs.python.org/issue7980
datetime.datetime.strptime("2013", "%Y")

EMAIL_TIMESTAMPS = []


def shutdown(default=5):
    """Shutdown method in given number of seconds."""
    # Careful, default could have been passed in as an error
    if not isinstance(default, int):
        LOG.error(default)
        delay = 5
    else:
        delay = (
            CTX.shutdown_delay if CTX.shutdown_delay is not None else default
        )
    LOG.info("Shutting down in %s seconds...", delay)
    reactor.callLater(delay, reactor.callFromThread, reactor.stop)


def utcnow():
    """Return what utcnow is based on command line."""
    return utc() if CTX.utcnow is None else CTX.utcnow


def dbwrite_enabled():
    """Is database writing not-disabled as per command line."""
    return not CTX.disable_dbwrite


def setup_syslog():
    """Setup how we want syslogging to work"""
    # https://stackoverflow.com/questions/13699283
    frame = inspect.stack()[-1]
    module = inspect.getmodule(frame[0])
    filename = "None" if module is None else os.path.basename(module.__file__)
    syslog.startLogging(
        prefix=f"pyWWA/{filename}",
        facility=LOG_LOCAL2,
        setStdout=not CTX.stdout_logging,
    )
    # pyIEM does logging via python stdlib logging, so we need to patch those
    # messages into twisted's logger.
    sh = logging.StreamHandler(stream=tplog.logfile)
    sh.setFormatter(CustomFormatter())
    LOG.addHandler(sh)
    # Log stuff to stdout if we are running from command line.
    if CTX.stdout_logging:
        tplog.addObserver(lambda x: print(formatEvent(x)))
    # Allow for more verbosity when we are running this manually.
    LOG.setLevel(logging.DEBUG if sys.stdout.isatty() else logging.INFO)


def load_settings():
    """Load database properties."""
    with get_sync_dbconn("mesosite") as dbconn:
        cursor = dbconn.cursor()
        cursor.execute("SELECT propname, propvalue from properties")
        for row in cursor:
            SETTINGS[row[0]] = row[1]
        LOG.info("Loaded %s settings from database", len(SETTINGS))
        cursor.close()


def should_email():
    """Prevent email bombs

    Use the setting `pywwa_email_limit` to threshold the number of emails
    permitted within the past hour

    @return boolean if we should email or not
    """
    EMAIL_TIMESTAMPS.insert(0, utc())
    delta = EMAIL_TIMESTAMPS[0] - EMAIL_TIMESTAMPS[-1]
    email_limit = int(SETTINGS.get("pywwa_email_limit", 10))
    if len(EMAIL_TIMESTAMPS) < email_limit:
        return True
    while len(EMAIL_TIMESTAMPS) > email_limit:
        EMAIL_TIMESTAMPS.pop()

    return delta > datetime.timedelta(hours=1)


def email_error(exp, message, trimstr=100):
    """
    Helper function to generate error emails when necessary and hopefully
    not flood!
    @param exp A string or perhaps a twisted python failure object
    @param message A string of more information to pass along in the email
    @return boolean If an email was sent or not...
    """
    # Always log a message about our fun
    cstr = StringIO()
    if isinstance(exp, failure.Failure):
        exp.printTraceback(file=cstr)
        LOG.error(exp)
    elif isinstance(exp, Exception):
        traceback.print_exc(file=cstr)
        LOG.error(exp)
    else:
        LOG.info(exp)
    cstr.seek(0)
    if isinstance(message, str):
        LOG.info(message[:trimstr])
    else:
        LOG.info(message)

    # Logic to prevent email bombs
    if not should_email():
        LOG.info(
            "Email threshold of %s exceeded, so no email sent!",
            SETTINGS.get("pywwa_email_limit", 10),
        )
        return False

    txt = """
System          : %s@%s [CWD: %s]
pyiem.version   : %s
System UTC date : %s
process id      : %s
system load     : %s
Exception       :
%s
%s

Message:
%s""" % (
        pwd.getpwuid(os.getuid())[0],
        socket.gethostname(),
        os.getcwd(),
        pyiem.__version__,
        utc(),
        os.getpid(),
        " ".join(["%.2f" % (_,) for _ in os.getloadavg()]),
        cstr.read(),
        exp,
        message,
    )
    # prevent any noaaport text from making ugly emails
    msg = MIMEText(txt.replace("\r\r\n", "\n"), "plain", "utf-8")
    # Send the email already!
    msg["subject"] = ("[pyWWA] %s Traceback -- %s") % (
        sys.argv[0].split("/")[-1],
        socket.gethostname(),
    )
    msg["From"] = SETTINGS.get("pywwa_errors_from", "ldm@localhost")
    msg["To"] = SETTINGS.get("pywwa_errors_to", "ldm@localhost")
    if not CTX.disable_email:
        df = smtp.sendmail(
            SETTINGS.get("pywwa_smtp", "smtp"), msg["From"], msg["To"], msg
        )
        df.addErrback(LOG.error)
    else:
        LOG.info("Sending email disabled by command line `-e` flag.")
    return True


# This is blocking, but necessary to make sure settings are loaded before
# we go on our merry way
CTX = parse_cmdline(sys.argv)
setup_syslog()
load_settings()
