'''

 Support lib for the parser scripts found in this directory

'''
#stdlib
import json
import os
import pwd
# http://bugs.python.org/issue7980
import datetime
datetime.datetime.strptime('2013', '%Y')
import re
import traceback
import StringIO
import socket
import sys
from email.MIMEText import MIMEText

#3rd party
import psycopg2
import pyiem

#twisted
from twisted.python import log
from twisted.python import failure
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
email_timestamps = []

def get_database(dbname, cp_max=5):
    """ Get a twisted database connection 
    
    Arguments:
    dbname -- The string name of the database to connect to
    cp_max -- The maximum number of connections to make to the database 
    """
    return adbapi.ConnectionPool("pyiem.twistedpg", database=dbname, 
                                cp_reconnect=True, cp_max=cp_max,
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
    cursor.execute(""" SELECT propname, propvalue from properties """)
    for row in cursor:
        settings[ row[0] ] = row[1]
    log.msg("common.load_settings loaded %s settings from database" % (
                                                            len(settings),))
    cursor.close()
    dbconn.close()

def should_email():
    ''' 
    Logic to prevent email bombs, we currently want no more than 10 per hour
    @return boolean if we should email or not
    '''
    utcnow = datetime.datetime.utcnow()
    email_timestamps.insert(0, utcnow )
    delta = email_timestamps[0] - email_timestamps[-1] 
    if len(email_timestamps) < 10:
        return True
    while len(email_timestamps) > 10:
        email_timestamps.pop()

    return (delta > datetime.timedelta(hours=1))
    
    

def email_error(exp, message):
    """
    Helper function to generate error emails when necessary and hopefully
    not flood!
    @param exp A string or perhaps a twisted python failure object
    @param message A string of more information to pass along in the email
    @return boolean If an email was sent or not...
    """
    # Always log a message about our fun
    cstr = StringIO.StringIO()
    if isinstance(exp, failure.Failure):
        traceback.print_exc(file=cstr)
        log.err(exp)
    cstr.seek(0)
    if type(message) == type(''):
        log.msg(message[:100])
    else:
        log.msg(message)

    # Logic to prevent email bombs
    if not should_email():
        log.msg("Email threshold exceeded, so no email sent!")
        return False

    msg = MIMEText("""
System          : %s@%s [CWD: %s]
pyiem.version   : %s
System UTC date : %s
process id      : %s
system load     : %s
Exception       :
%s
%s

Message:
%s""" % (pwd.getpwuid(os.getuid())[0], socket.gethostname(), os.getcwd(),
         pyiem.__version__, 
         datetime.datetime.utcnow(),
         os.getpid(), ' '.join(['%.2f' % (_,) for _ in os.getloadavg()]), 
         cstr.read(), exp, message))

    # Send the email already!
    msg['subject'] = '[pyWWA] %s Traceback -- %s' % (
                    sys.argv[0].split("/")[-1], socket.gethostname())
    msg['From'] = settings.get('pywwa_errors_from', 'ldm@localhost')
    msg['To'] = settings.get('pywwa_errors_to', 'ldm@localhost')
    df = smtp.sendmail(settings.get('pywwa_smtp', 'smtp'), 
                  msg["From"], msg["To"], msg)
    df.addErrback( log.err )
    return True

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
                settings.get('bot.username', 'nwsbot'),
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
