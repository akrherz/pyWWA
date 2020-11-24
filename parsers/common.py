"""Support lib for the parser scripts found in this directory"""
from __future__ import print_function
import argparse
import json
import os
import inspect
import logging
import pwd
import datetime
import re
from io import StringIO
import socket
import sys
import traceback
from email.mime.text import MIMEText
from syslog import LOG_LOCAL2

# 3rd party
import psycopg2

# twisted
from twisted.python import log
from twisted.logger import formatEvent
from twisted.python import syslog
from twisted.python import failure
from twisted.words.xish.xmlstream import STREAM_END_EVENT
from twisted.words.protocols.jabber import client as jclient
from twisted.words.protocols.jabber import xmlstream, jid
from twisted.internet.task import LoopingCall
from twisted.internet import reactor
from twisted.words.xish import domish, xpath
from twisted.mail import smtp
from twisted.enterprise import adbapi
import pyiem
from pyiem.util import LOG, utc

# http://bugs.python.org/issue7980
datetime.datetime.strptime("2013", "%Y")
MYREGEX = "[\x00-\x08\x0b\x0c\x0e-\x1F\uD800-\uDFFF\uFFFE\uFFFF]"
ILLEGAL_XML_CHARS_RE = re.compile(MYREGEX)

SETTINGS = {}
EMAIL_TIMESTAMPS = []
# Careful modifying this, be sure to test from LDM account
CONFIG = json.load(
    open(os.path.join(os.path.dirname(__file__), "../settings.json"))
)


def shutdown(default=5):
    """Shutdown method in given number of seconds."""
    delay = CTX.shutdown_delay if CTX.shutdown_delay is not None else default
    log.msg(f"Shutting down in {delay} seconds...")
    reactor.callLater(delay, reactor.callFromThread, reactor.stop)


def utcnow():
    """Return what utcnow is based on command line."""
    return utc() if CTX.utcnow is None else CTX.utcnow


def dbwrite_enabled():
    """Is database writing not-disabled as per command line."""
    return not CTX.disable_dbwrite


def parse_cmdline():
    """Parse command line for context settings."""
    parser = argparse.ArgumentParser(description="pyWWA Parser.")
    parser.add_argument(
        "-d",
        "--disable-dbwrite",
        action="store_true",
        help=(
            "Disable any writing to databases, still may need read access "
            "to initialize metadata tables."
        ),
    )
    parser.add_argument(
        "-e",
        "--disable-email",
        action="store_true",
        help="Disable sending any emails.",
    )
    parser.add_argument(
        "-l",
        "--stdout-logging",
        action="store_true",
        help="Also log to stdout.",
    )
    parser.add_argument(
        "-s",
        "--shutdown-delay",
        type=int,
        help=(
            "Number of seconds to wait before shutting down process when "
            "STDIN is closed to the process.  0 is immediate."
        ),
    )

    def _parsevalid(val):
        """Convert to datetime."""
        v = datetime.datetime.strptime(val[:16], "%Y-%m-%dT%H:%M")
        return v.replace(tzinfo=datetime.timezone.utc)

    parser.add_argument(
        "-u",
        "--utcnow",
        type=_parsevalid,
        metavar="YYYY-MM-DDTHH:MI",
        help="Provide the current UTC Timestamp (defaults to realtime.).",
    )
    parser.add_argument(
        "-x",
        "--disable-xmpp",
        action="store_true",
        help="Disable all XMPP functionality.",
    )
    return parser.parse_args(sys.argv[1:])


def setup_syslog():
    """Setup how we want syslogging to work"""
    # https://stackoverflow.com/questions/13699283
    frame = inspect.stack()[-1]
    module = inspect.getmodule(frame[0])
    filename = os.path.basename(module.__file__)
    syslog.startLogging(
        prefix=f"pyWWA/{filename}",
        facility=LOG_LOCAL2,
        setStdout=not CTX.stdout_logging,
    )
    # pyIEM does logging via python stdlib logging, so we need to patch those
    # messages into twisted's logger.
    LOG.addHandler(logging.StreamHandler(stream=log.logfile))
    # Log stuff to stdout if we are running from command line.
    if CTX.stdout_logging:
        log.addObserver(lambda x: print(formatEvent(x)))
        log.msg("Copying logging to stdout.")
    # Allow for more verbosity when we are running this manually.
    LOG.setLevel(logging.DEBUG if sys.stdout.isatty() else logging.INFO)


def get_database(dbname, cp_max=5, module_name="pyiem.twistedpg"):
    """Get a twisted database connection

    Args:
      dbname (str): The string name of the database to connect to
      cp_max (int): The maximum number of connections to make to the database
      module_name (str): The python module to use for the ConnectionPool
    """
    host = "iemdb-%s.local" % (dbname,)
    return adbapi.ConnectionPool(
        module_name,
        database=dbname,
        cp_reconnect=True,
        cp_max=cp_max,
        host=host,
        user=CONFIG.get("databaserw").get("user"),
        gssencmode="disable",
    )


def load_settings():
    """Load settings immediately, so we don't have to worry about the settings
    not being loaded for subsequent usage"""

    dbconn = psycopg2.connect(
        database=CONFIG.get("databasero").get("openfire"),
        host=CONFIG.get("databasero").get("host"),
        password=CONFIG.get("databasero").get("password"),
        user=CONFIG.get("databasero").get("user"),
    )
    cursor = dbconn.cursor()
    cursor.execute("SELECT propname, propvalue from properties")
    for row in cursor:
        SETTINGS[row[0]] = row[1]
    log.msg(
        f"common.load_settings loaded {len(SETTINGS)} settings from database"
    )
    cursor.close()
    dbconn.close()


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
        log.err(exp)
    elif isinstance(exp, Exception):
        traceback.print_exc(file=cstr)
        log.err(exp)
    else:
        log.msg(exp)
    cstr.seek(0)
    if isinstance(message, str):
        log.msg(message[:trimstr])
    else:
        log.msg(message)

    # Logic to prevent email bombs
    if not should_email():
        log.msg(
            ("Email threshold of %s exceeded, so no email sent!")
            % (SETTINGS.get("pywwa_email_limit", 10))
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
        df.addErrback(log.err)
    else:
        log.msg("Sending email disabled by command line `-e` flag.")
    return True


def make_jabber_client(resource_prefix):
    """ Generate a jabber client, please """
    if CTX.disable_xmpp:
        LOG.info("XMPP disabled via command line.")
        return NOOPXMPP()

    myjid = jid.JID(
        "%s@%s/%s_%s"
        % (
            SETTINGS.get("pywwa_jabber_username", "nwsbot_ingest"),
            SETTINGS.get("pywwa_jabber_domain", "nwschat.weather.gov"),
            resource_prefix,
            utc().strftime("%Y%m%d%H%M%S"),
        )
    )
    factory = jclient.XMPPClientFactory(
        myjid, SETTINGS.get("pywwa_jabber_password", "secret")
    )

    jabber = JabberClient(myjid)

    factory.addBootstrap("//event/stream/authd", jabber.authd)
    factory.addBootstrap("//event/client/basicauth/invaliduser", debug)
    factory.addBootstrap("//event/client/basicauth/authfailed", debug)
    factory.addBootstrap("//event/stream/error", debug)
    factory.addBootstrap(xmlstream.STREAM_END_EVENT, jabber.disconnect)

    reactor.connectTCP(
        SETTINGS.get("pywwa_jabber_host", "localhost"),  # @UndefinedVariable
        5222,
        factory,
    )

    return jabber


def message_processor(stanza):
    """ Process a message stanza """
    body = xpath.queryForString("/message/body", stanza)
    log.msg("Message from %s Body: %s" % (stanza["from"], body))
    if body is None:
        return
    if body.lower().strip() == "shutdown":
        log.msg("I got shutdown message, shutting down...")
        reactor.callWhenRunning(reactor.stop)  # @UndefinedVariable


def raw_data_in(data):
    """
    Debug method
    @param data string of what was received from the server
    """
    if isinstance(data, bytes):
        data = data.decode("utf-8", errors="ignore")
    log.msg("RECV %s" % (data,))


def debug(elem):
    """
    Debug method
    @param elem twisted.works.xish
    """
    log.msg(elem.toXml().encode("utf-8"))


def raw_data_out(data):
    """
    Debug method
    @param data string of what data was sent
    """
    if isinstance(data, bytes):
        data = data.decode("utf-8", errors="ignore")
    if data == " ":
        return
    log.msg("SEND %s" % (data,))


class NOOPXMPP:
    """A no-operation Jabber client."""

    def keepalive(self):
        """Do Nothing."""

    def send_message(self, *args):
        """Do Nothing."""


class JabberClient(object):
    """
    I am an important class of a jabber client against the chat server,
    used by pretty much every ingestor as that is how messages are routed
    to nwsbot
    """

    def __init__(self, myjid):
        """
        Constructor

        @param jid twisted.words.jid object
        """
        self.myjid = myjid
        self.xmlstream = None
        self.authenticated = False
        self.routerjid = "%s@%s" % (
            SETTINGS.get("bot.username", "nwsbot"),
            SETTINGS.get("pywwa_jabber_domain", "nwschat.weather.gov"),
        )

    def authd(self, xstream):
        """
        Callbacked once authentication succeeds
        @param xs twisted.words.xish.xmlstream
        """
        log.msg("Logged in as %s" % (self.myjid,))
        self.authenticated = True

        self.xmlstream = xstream
        self.xmlstream.rawDataInFn = raw_data_in
        self.xmlstream.rawDataOutFn = raw_data_out

        # Process Messages
        self.xmlstream.addObserver("/message/body", message_processor)

        # Send initial presence
        presence = domish.Element(("jabber:client", "presence"))
        presence.addElement("status").addContent("Online")
        self.xmlstream.send(presence)

        # Whitespace ping to keep our connection happy
        lc = LoopingCall(self.keepalive)
        lc.start(60)
        self.xmlstream.addObserver(STREAM_END_EVENT, lambda _: lc.stop())

    def keepalive(self):
        """
        Send whitespace ping to the server every so often
        """
        self.xmlstream.send(" ")

    def disconnect(self, _xs):
        """
        Called when we are disconnected from the server, I guess
        """
        log.msg("SETTING authenticated to false!")
        self.authenticated = False

    def send_message(self, body, html, xtra):
        """
        Send a message to nwsbot.  This message should have
        @param body plain text variant
        @param html html version of the message
        @param xtra dictionary of stuff that tags along
        """
        if not self.authenticated:
            log.msg("No Connection, Lets wait and try later...")
            reactor.callLater(
                3, self.send_message, body, html, xtra  # @UndefinedVariable
            )
            return
        message = domish.Element(("jabber:client", "message"))
        message["to"] = self.routerjid
        message["type"] = "chat"

        # message.addElement('subject',None,subject)
        body = ILLEGAL_XML_CHARS_RE.sub("", body)
        if html:
            html = ILLEGAL_XML_CHARS_RE.sub("", html)
        message.addElement("body", None, body)
        helem = message.addElement(
            "html", "http://jabber.org/protocol/xhtml-im"
        )
        belem = helem.addElement("body", "http://www.w3.org/1999/xhtml")
        belem.addRawXml(html or body)
        # channels is of most important
        xelem = message.addElement("x", "nwschat:nwsbot")
        for key in xtra.keys():
            if isinstance(xtra[key], list):
                xelem[key] = ",".join(xtra[key])
            else:
                xelem[key] = xtra[key]
        # So send_message may be getting called from 'threads' and the writing
        # of data to the transport is not thread safe, so we must ensure that
        # this gets called from the main thread
        reactor.callFromThread(
            self.xmlstream.send, message  # @UndefinedVariable
        )


# This is blocking, but necessary to make sure settings are loaded before
# we go on our merry way
CTX = parse_cmdline()
setup_syslog()
load_settings()
