"""Support lib for the parser scripts found in this directory"""

import datetime
import getpass
import inspect
import logging
import os
import socket
import sys
import traceback
from email.mime.text import MIMEText
from io import StringIO

import click
import pyiem
from pyiem.util import utc
from twisted.internet import reactor
from twisted.logger import formatEvent
from twisted.mail import smtp
from twisted.python import failure
from twisted.python import log as tplog

# Local Be careful of circeref here
from pywwa import CTX, LOG, SETTINGS
from pywwa.database import get_dbconn

# http://bugs.python.org/issue7980
datetime.datetime.strptime("2013", "%Y")
EMAIL_TIMESTAMPS = []


class CustomFormatter(logging.Formatter):
    """A custom log formatter class."""

    def format(self, record):
        """Return a string!"""
        return (
            f"[{record.filename}:{record.lineno} {record.funcName}] "
            f"{record.getMessage()}"
        )


def utcnow():
    """Return what utcnow is based on command line."""
    return utc() if CTX["utcnow"] is None else CTX["utcnow"]


def dbwrite_enabled():
    """Is database writing not-disabled as per command line."""
    return not CTX["disable_dbwrite"]


def replace_enabled():
    """Is -r --replace enabled."""
    return CTX["replace"]


def setup_syslog():
    """Setup how we want syslogging to work"""
    # I am punting on some issue the below creates within pytest
    if SETTINGS.get("__setup_syslog", False):
        return
    # https://stackoverflow.com/questions/13699283
    frame = inspect.stack()[-1]
    module = inspect.getmodule(frame[0])
    filename = "None" if module is None else os.path.basename(module.__file__)
    # windows does not have syslog
    try:
        from twisted.python import syslog

        syslog.startLogging(
            prefix=f"pyWWA/{filename}",
            facility=syslog.syslog.LOG_LOCAL2,
            setStdout=not CTX["stdout_logging"],
        )
    except ImportError:
        LOG.info("Failed to import twisted.python.syslog")
    # pyIEM does logging via python stdlib logging, so we need to patch those
    # messages into twisted's logger.
    sh = logging.StreamHandler(stream=tplog.logfile)
    sh.setFormatter(CustomFormatter())
    LOG.addHandler(sh)
    # Log stuff to stdout if we are running from command line.
    if CTX["stdout_logging"]:
        tplog.addObserver(lambda x: sys.stdout.write(formatEvent(x) + "\n"))
    # Allow for more verbosity when we are running this manually.
    LOG.setLevel(logging.DEBUG if sys.stdout.isatty() else logging.INFO)
    SETTINGS["__setup_syslog"] = True


def load_settings():
    """Load database properties."""
    with get_dbconn("mesosite") as dbconn:
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


def email_error(exp, message="", trimstr=100):
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

    hn = socket.gethostname()
    hh = f"{getpass.getuser()}@{hn}"
    la = " ".join([f"{a:.2f}" for a in os.getloadavg()])
    txt = (
        f"System          : {hh} [CWD: {os.getcwd()}]\n"
        f"pyiem.version   : {pyiem.__version__}\n"
        f"System UTC date : {utc()}\n"
        f"pyWWA UTC date  : {utcnow()}\n"
        f"process id      : {os.getpid()}\n"
        f"system load     : {la}\n"
        f"Exception       : {exp}\n"
        f"Message:\n{message}\n"
    )
    # prevent any noaaport text from making ugly emails
    msg = MIMEText(txt.replace("\r\r\n", "\n"), "plain", "utf-8")
    # Send the email already!
    msg["subject"] = (
        f"[pyWWA] {sys.argv[0].rsplit('/', maxsplit=1)[-1]} Traceback -- {hn}"
    )
    msg["From"] = SETTINGS.get("pywwa_errors_from", "ldm@localhost")
    msg["To"] = SETTINGS.get("pywwa_errors_to", "ldm@localhost")
    if not CTX["disable_email"]:
        df = smtp.sendmail(
            SETTINGS.get("pywwa_smtp", "smtp"), msg["From"], msg["To"], msg
        )
        df.addErrback(LOG.error)
    else:
        LOG.info("Sending email disabled by command line `-e` flag.")
    return True


def send_message(plain, text, extra):
    """Helper to connect with running JABBER instance."""
    if CTX["JABBER"] is None:
        LOG.info("failed to send as pywwa.JABBER is None, not setup?")
        return
    CTX["JABBER"].send_message(plain, text, extra)


def parse_utcnow(text) -> datetime.datetime:
    """Parse a string into a datetime object."""
    if text is None:
        return None
    fmt = "%Y-%m-%dT%H:%M" if len(text) == 16 else "%Y-%m-%dT%H:%M:%SZ"
    dt = datetime.datetime.strptime(text, fmt)
    return dt.replace(tzinfo=datetime.timezone.utc)


def disable_xmpp(f):
    """Decorator to disable XMPP."""
    f.disable_xmpp = True
    return f


def init(f):
    """Decorator to setup all things."""

    @click.option(
        "-d",
        "--disable-dbwrite",
        is_flag=True,
        help="Disable writing to the database",
    )
    @click.option(
        "-e",
        "--disable-email",
        is_flag=True,
        help="Disable sending emails",
    )
    @click.option(
        "-l",
        "--stdout-logging",
        is_flag=True,
        help="Log to stdout",
    )
    @click.option(
        "-r",
        "--replace",
        is_flag=True,
        help="Replace existing database entries",
    )
    @click.option(
        "-s",
        "--shutdown-delay",
        type=int,
        default=5,
        help="Shutdown after N seconds",
    )
    @click.option(
        "-u",
        "--utcnow",
        help="Define current UTC timestamp",
        type=parse_utcnow,
    )
    @click.option(
        "-x",
        "--disable-xmpp",
        default=getattr(f, "disable_xmppp", False),
        is_flag=True,
        help="Disable XMPP messaging",
    )
    def decorated_function(*args, **kwargs):
        """Decorated function."""
        # Step 1, parse command line arguments into running context
        CTX.update(kwargs)
        # Step 2, setup logging
        setup_syslog()
        # Step 3, load settings from database, which is blocking
        load_settings()
        # Step 4, setup jabber client
        if not CTX["disable_xmpp"] and not getattr(f, "disable_xmpp", False):
            from pywwa.xmpp import make_jabber_client

            make_jabber_client()
        # Step 5, call the function
        ret = f(*args, **kwargs)
        # Step 6, run the reactor, but account for ugliness of pytest
        if not reactor.running:
            reactor.run()
        return ret

    return decorated_function
