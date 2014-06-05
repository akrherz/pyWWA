'''

 Support lib for the parser scripts found in this directory

'''
#stdlib
import json
import os
import datetime
import re
import traceback
import StringIO
import socket
import sys
from email.MIMEText import MIMEText

#3rd party
import psycopg2

#twisted
from twisted.python import log
from twisted.words.xish.xmlstream import STREAM_END_EVENT
from twisted.words.protocols.jabber import client as jclient
from twisted.words.protocols.jabber import xmlstream, jid
from twisted.internet.task import LoopingCall
from twisted.internet import reactor
from twisted.words.xish import domish, xpath
from twisted.mail import smtp
from twisted.enterprise import adbapi

_illegal_xml_chars_RE = re.compile(
                    u'[\x00-\x08\x0b\x0c\x0e-\x1F\uD800-\uDFFF\uFFFE\uFFFF]')


config = json.load(open(os.path.join(os.path.dirname(__file__),
                                     '../settings.json')))
settings = {}

def get_database(dbname):
    ''' Get a database database connection '''
    return adbapi.ConnectionPool("pyiem.twistedpg", database=dbname, 
                                cp_reconnect=True,
                                host=config.get('databaserw').get('host'), 
                                user=config.get('databaserw').get('user'),
                                password=config.get('databaserw').get('password')) 

def load_settings():
    """ Load settings immediately, so we don't have to worry about the settings 
        not being loaded for subsequent usage """

    dbconn = psycopg2.connect(database=config.get('databasero').get('openfire'),
                            host=config.get('databasero').get('host'),
                            password=config.get('databasero').get('password'),
                            user=config.get('databasero').get('user'))
    cursor = dbconn.cursor()
    cursor.execute(""" SELECT propname, propvalue from nwschat_properties """)
    for row in cursor:
        settings[ row[0] ] = row[1]
    log.msg("common.load_settings loaded %s settings from database" % (
                                                            len(settings),))
    cursor.close()
    dbconn.close()

def email_error(exp, message):
    """
    Helper function to generate error emails when necessary and hopefully
    not flood!
    @param exp A string or perhaps a traceback object
    @param message A string of more information to pass along in the email
    """
    # Always log a message about our fun
    cstr = StringIO.StringIO()
    traceback.print_exc(file=cstr)
    cstr.seek(0)
    tbstr = cstr.read()
    log.err( exp )
    log.msg( message )

    # Prevent a parser from sending tons of spam
    if int(os.environ.get('EMAILS', 10)) < 0:
        return
    os.environ['EMAILS'] = str( int(os.environ.get("EMAILS",10)) - 1 )

    msg = MIMEText("""
Emails Left: %s  Host: %s

Exception:
%s
%s

Message:
%s""" % (os.environ["EMAILS"], socket.gethostname(), tbstr, exp, message))
    # Send the email already!
    msg['subject'] = '%s Traceback' % (sys.argv[0],)
    msg['From'] = settings.get('pywwa_errors_from', 'ldm@localhost')
    msg['To'] = settings.get('pywwa_errors_to', 'ldm@localhost')
    smtp.sendmail("smtp", msg["From"], msg["To"], msg)


def write_pid(myname):
    """ Create and write a PID file for this process """
    o = open("%s.pid" % (myname,),'w')
    o.write("%s" % ( os.getpid(),) )
    o.close()


def make_jabber_client(resource_prefix):
    """ Generate a jabber client, please """

    myJid = jid.JID('%s@%s/%s_%s' % (
                    settings.get('pywwa_jabber_username', 'nwsbot_ingest'),
                    settings.get('pywwa_jabber_domain', 'nwschat.weather.gov'),
                    resource_prefix,
                    datetime.datetime.utcnow().strftime("%Y%m%d%H%M%S") ) )
    factory = jclient.basicClientFactory(myJid,
                    settings.get('pywwa_jabber_password', 'secret'))

    jabber = JabberClient(myJid)

    factory.addBootstrap('//event/stream/authd', jabber.authd)
    factory.addBootstrap("//event/client/basicauth/invaliduser", jabber.debug)
    factory.addBootstrap("//event/client/basicauth/authfailed", jabber.debug)
    factory.addBootstrap("//event/stream/error", jabber.debug)
    factory.addBootstrap(xmlstream.STREAM_END_EVENT, jabber._disconnect )

    reactor.connectTCP(settings.get('pywwa_jabber_host', 'localhost'), 5222,
                       factory)

    return jabber

class JabberClient:
    """
    I am an important class of a jabber client against the chat server,
    used by pretty much every ingestor as that is how messages are routed
    to nwsbot
    """

    def __init__(self, myJid):
        """
        Constructor 
        
        @param myJid twisted.words.jid object
        """
        self.myJid = myJid
        self.xmlstream = None
        self.authenticated = False
        self.routerJid = "%s@%s" % (
                settings.get('nwsbot_jabber_username', 'nwsbot'),
                settings.get('pywwa_jabber_domain', 'nwschat.weather.gov'))

    def has_football(self):
        """
        Check to make sure we have the football before doing important things
        """
        return os.path.isfile("/home/nwschat/public_html/service.no")

    def message_processor(self, stanza):
        """ Process a message stanza """
        body = xpath.queryForString("/message/body", stanza)
        log.msg("Message from %s Body: %s" % (stanza['from'], body))
        if body is None:
            return
        if body.lower().strip() == "shutdown":
            log.msg("I got shutdown message, shutting down...")
            reactor.callWhenRunning( reactor.stop )

    def authd(self,xs):
        """
        Callbacked once authentication succeeds
        @param xs twisted.words.xish.xmlstream
        """
        log.msg("Logged in as %s" % (self.myJid,))
        self.authenticated = True

        self.xmlstream = xs
        self.xmlstream.rawDataInFn = self.rawDataInFn
        self.xmlstream.rawDataOutFn = self.rawDataOutFn

        # Process Messages
        self.xmlstream.addObserver('/message/body',  self.message_processor)

        # Send initial presence
        presence = domish.Element(('jabber:client','presence'))
        presence.addElement('status').addContent('Online')
        self.xmlstream.send(presence)

        # Whitespace ping to keep our connection happy
        lc = LoopingCall(self.keepalive)
        lc.start(60)
        self.xmlstream.addObserver(STREAM_END_EVENT, lambda _: lc.stop())

    def keepalive(self):
        """
        Send whitespace ping to the server every so often
        """
        self.xmlstream.send(' ')

    def _disconnect(self, xs):
        """
        Called when we are disconnected from the server, I guess
        """
        log.msg("SETTING authenticated to false!")
        self.authenticated = False

    def sendMessage(self, body, html, xtra):
        """
        Send a message to nwsbot.  This message should have
        @param body plain text variant
        @param html html version of the message
        @param xtra dictionary of stuff that tags along
        """
        if not self.authenticated:
            log.msg("No Connection, Lets wait and try later...")
            reactor.callLater(3, self.sendMessage, body, html, xtra)
            return
        message = domish.Element(('jabber:client','message'))
        message['to'] = self.routerJid
        message['type'] = 'chat'

        # message.addElement('subject',None,subject)
        body = _illegal_xml_chars_RE.sub('', body)
        if html:
            html = _illegal_xml_chars_RE.sub('', html)
        message.addElement('body',None,body)
        h = message.addElement('html','http://jabber.org/protocol/xhtml-im')
        b = h.addElement('body', 'http://www.w3.org/1999/xhtml')
        b.addRawXml( html or body )
        # channels is of most important
        x = message.addElement('x', 'nwschat:nwsbot')
        for key in xtra.keys():
            x[key] = xtra[key]
        self.xmlstream.send(message)


    def debug(self, elem):
        """
        Debug method
        @param elem twisted.works.xish
        """
        log.msg( elem.toXml().encode('utf-8') )

    def rawDataInFn(self, data):
        """
        Debug method 
        @param data string of what was received from the server
        """
        log.msg('RECV %s' % (unicode(data,'utf-8','ignore').encode('ascii', 
                                                                'replace'),))

    def rawDataOutFn(self, data):
        """
        Debug method
        @param data string of what data was sent
        """
        if (data == ' '): return
        log.msg('SEND %s' % (unicode(data,'utf-8','ignore').encode('ascii', 
                                                                'replace'),))
        
# This is blocking, but necessary to make sure settings are loaded before
# we go on our merry way
load_settings()
