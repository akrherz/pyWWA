# Common stuff for the pyWWA ingestors...

from twisted.internet import reactor
from twisted.words.xish import domish
from twisted.python import log
from twisted.mail import smtp
from twisted.web import client
import twisted.web.error as weberror
from twisted.words.xish.xmlstream import STREAM_END_EVENT
from twisted.words.protocols.jabber import client as jclient
from twisted.words.protocols.jabber import xmlstream, jid
from twisted.internet.task import LoopingCall
from twisted.internet import task
from twisted.enterprise import adbapi

from twittytwister import twitter
import simplejson
import traceback
import datetime
import sys
import os
import StringIO
from email.MIMEText import MIMEText
import socket
from oauth import oauth

import ConfigParser
config = ConfigParser.ConfigParser()
config.read(os.path.join(os.path.dirname(__file__), 'cfg.ini'))


OAUTH_TOKENS = {}
OAUTH_CONSUMER = oauth.OAuthConsumer(config.get('twitter','consumerkey'), 
                                     config.get('twitter','consumersecret'))
BITLY = "http://api.bit.ly/shorten?version=2.0.1&longUrl=%s&login=iembot&apiKey="+ config.get('bitly','apikey')


def load_tokens(txn):
    txn.execute("SELECT * from oauth_tokens")
    for row in txn:
        OAUTH_TOKENS[ row['username'] ] = oauth.OAuthToken(
                            row['token'], row['secret'])

_dbpool = adbapi.ConnectionPool("twistedpg", database="mesosite", cp_reconnect=True,
                                host=config.get('database','host'), 
                                user=config.get('database','user'),
                                password=config.get('database','password') )
defer = _dbpool.runInteraction(load_tokens)
def stop_pool(res):
    _dbpool.close()
defer.addCallback(stop_pool)

def email_error(exp, message):
    """
    Helper function to generate error emails when necessary and hopefully
    not flood!
    """
    # Always log a message about our fun
    cstr = StringIO.StringIO()
    traceback.print_exc(file=cstr)
    cstr.seek(0)
    tbstr = cstr.read()
    log.msg( tbstr )
    log.msg( exp )
    log.msg( message )

    # Now, we may email....
    if os.environ.get('EMAILS') is None:
        os.environ['EMAILS'] = "10"
    if int(os.environ['EMAILS']) < 0:
        log.msg("NO EMAIL DUE TO LIMIT...")
        return
    os.environ['EMAILS'] = str( int(os.environ["EMAILS"]) - 1 )

    msg = MIMEText("""
Emails Left: %s  Host: %s

Exception:
%s
%s

Message:
%s
""" % (os.environ["EMAILS"], socket.gethostname(), tbstr, exp, message))
    msg['subject'] = '%s Traceback' % (sys.argv[0],)
    msg['From'] = config.get('errors', 'emailfrom')
    msg['To'] = config.get('errors', 'emailfrom')
    smtp.sendmail("localhost", msg["From"], msg["To"], msg)

def tweet(channels, msg, url, extras={}):
    """
    Method to publish twitter messages
    """
    # Simple hack to see if we are running in development TODO
    if config.get('xmpp','domain') == 'laptop.local':
        channels = ['TEST',]
        #return
    
    if url:
        url = url.replace("&amp;", "&").replace("#","%23").replace("&","%26").replace("?", "%3F")
        deffer = client.getPage(BITLY % (url, ) )
        deffer.addCallback(reallytweet, channels, msg, extras )
        deffer.addErrback(bitly_error, channels, msg, extras )
        deffer.addErrback(log.err)
    else:
        reallytweet(None, channels, msg, extras)

def bitly_error(err, channels, msg, extras):
    """
    Sometimes bad things happen with bitly
    """
    log.msg("Encountered BITLY error")
    err.trap( weberror.Error )
    log.msg( err.getErrorMessage() )
    log.msg( err.value.response )
    tweet_step2(channels, msg, extras)

def reallytweet(json, channels, msg, extras):
    """
    Actually, really publish this time!
    """
    tinyurl = ""
    if json and type(json) == type(""):
        j = simplejson.loads( json )
        if j.has_key('errorCode') and j['errorCode'] != 0 and j.has_key('errorMessage'):
            if j['errorCode'] != 500:
                email_error(str(j), "Problem with bitly")
        elif j.has_key('results'):
            tinyurl = j['results'][ j['results'].keys()[0] ]['shortUrl']
    tweet_step2(channels, msg, extras, tinyurl)

def tweet_step2(channels, msg, extras, tinyurl=""):
    i = 0
    for channel in channels:
        tuser = "iembot_%s" % (channel.lower(),)
        if not OAUTH_TOKENS.has_key(tuser):
            log.msg("Unknown Twitter User, %s" % (tuser,))
            continue
        df = task.deferLater(reactor, i, really_really_tweet, 
                          tuser, channel, tinyurl, msg, extras)
        df.addErrback( log.err )
        i += 1

def make_jabber_client(resource_prefix):
    """ Generate a jabber client, please """

    myJid = jid.JID('%s@%s/%s_%s' % (config.get('xmpp','username'), 
                                    config.get('xmpp','domain'), resource_prefix,
       datetime.datetime.utcnow().strftime("%Y%m%d%H%M%S") ) )
    factory = jclient.basicClientFactory(myJid, config.get('xmpp', 'password'))

    jabber = JabberClient(myJid)

    factory.addBootstrap('//event/stream/authd', jabber.authd)
    factory.addBootstrap("//event/client/basicauth/invaliduser", jabber.debug)
    factory.addBootstrap("//event/client/basicauth/authfailed", jabber.debug)
    factory.addBootstrap("//event/stream/error", jabber.debug)
    factory.addBootstrap(xmlstream.STREAM_END_EVENT, jabber._disconnect )

    reactor.connectTCP(config.get('xmpp', 'connecthost'), 5222, factory)

    return jabber

        
def really_really_tweet(tuser, channel, tinyurl, msg, extras):
    """
    Throttled twitter calls
    """
    if tinyurl != "":
        twt = "#%s %s %s?%s" % (channel, msg[:108], tinyurl, channel)
    else:
        twt = "#%s %s" % (channel, msg[:118])
    _twitter = twitter.Twitter(consumer=OAUTH_CONSUMER, 
                               token=OAUTH_TOKENS[tuser])
    deffer = _twitter.update( twt[:140], None, extras)
    deffer.addCallback(tb, tuser, channel, twt)
    deffer.addErrback(twitterErrback, tuser, channel, twt)
    deffer.addErrback(log.err)

def tb(x, tuser, channel, twt):
    log.msg("TWEET User: %s TWT: [%s] RES: %s" % (tuser, twt, x) )
    
def twitterErrback(error, tuser, channel, twt):
    """
    Errback for twitter calls! 
    """
    error.trap( weberror.Error )
    log.msg("TWEET ERROR User: %s TWT: [%s] RES: %s" % (tuser, twt, error) )
    log.msg( error.getErrorMessage() )
    if error.value.status == "401":
        email_error(error.value.response, "Unauthorized 401 for %s" % (tuser,))


class JabberClient:

    def __init__(self, myJid):
        self.myJid = myJid
        self.xmlstream = None
        self.authenticated = False

    def authd(self,xs):
        log.msg("Logged into Jabber Chat Server!")

        self.xmlstream = xs
        self.xmlstream.rawDataInFn = self.rawDataInFn
        self.xmlstream.rawDataOutFn = self.rawDataOutFn
        presence = domish.Element(('jabber:client','presence'))
        presence.addElement('status').addContent('Online')
        self.xmlstream.send(presence)
        self.authenticated = True

        lc = LoopingCall(self.keepalive)
        lc.start(60)
        self.xmlstream.addObserver(STREAM_END_EVENT, lambda _: lc.stop())

    def keepalive(self):
        self.xmlstream.send(' ')

    def _disconnect(self, xs):
        log.msg("SETTING authenticated to false!")
        self.authenticated = False

    def sendMessage(self, body, html, to_user=config.get('xmpp', 'iembot')):
        if (not self.authenticated):
            log.msg("No Connection, Lets wait and try later...")
            reactor.callLater(3, self.sendMessage, body, html, to_user)
            return
        message = domish.Element(('jabber:client','message'))
        message['to'] = '%s@%s' % (to_user, config.get('xmpp', 'domain'))
        message['type'] = 'chat'

        # message.addElement('subject',None,subject)
        message.addElement('body',None,body)
        h = message.addElement('html','http://jabber.org/protocol/xhtml-im')
        b = h.addElement('body', 'http://www.w3.org/1999/xhtml')
        b.addRawXml( html or body )
        self.xmlstream.send(message)

    def debug(self, elem):
        log.msg( elem.toXml().encode('utf-8') )

    def rawDataInFn(self, data):
        print 'RECV', unicode(data,'utf-8','ignore').encode('ascii', 'replace')
    def rawDataOutFn(self, data):
        if (data == ' '): return 
        print 'SEND', unicode(data,'utf-8','ignore').encode('ascii', 'replace')
