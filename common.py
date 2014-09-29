''' Common stuff for the pyWWA ingestors...'''

#twisted stuff
from twisted.internet import reactor
from twisted.words.xish import domish
from twisted.python import log
from twisted.mail import smtp
import twisted.web.error as weberror
from twisted.words.xish.xmlstream import STREAM_END_EVENT
from twisted.words.protocols.jabber import client as jclient
from twisted.words.protocols.jabber import xmlstream, jid
from twisted.internet.task import LoopingCall
from twisted.internet import task
from twisted.enterprise import adbapi

#stdlib
import json
import time
import traceback
import datetime
import sys
import os
import StringIO
from email.mime.text import MIMEText
import socket
from oauth import oauth
import ConfigParser

#third party
from twittytwister import twitter

config = ConfigParser.ConfigParser()
config.read(os.path.join(os.path.dirname(__file__), 'cfg.ini'))


OAUTH_TOKENS = {}
OAUTH_CONSUMER = oauth.OAuthConsumer(config.get('twitter','consumerkey'), 
                                     config.get('twitter','consumersecret'))

def get_database(dbname, cp_max=5):
    ''' Get a database database connection '''
    return adbapi.ConnectionPool("pyiem.twistedpg", database=dbname, 
                                cp_reconnect=True, cp_max=cp_max,
                                host=config.get('database','host'), 
                                user=config.get('database','user'),
                                password=config.get('database','password')) 

def load_tokens(txn):
    ''' Load twitter access tokens '''
    log.msg("Loading oauth_tokens...")
    txn.execute("SELECT * from iembot_twitter_oauth")
    for row in txn:
        OAUTH_TOKENS[ row['screen_name'] ] = oauth.OAuthToken(
                            row['access_token'], row['access_token_secret'])
    log.msg("... loaded %s oauth_tokens." % (len(OAUTH_TOKENS.keys()),))

_dbpool = adbapi.ConnectionPool("pyiem.twistedpg", database="mesosite", 
                                cp_reconnect=True, cp_max=1,
                                host=config.get('database','host'), 
                                user=config.get('database','user'),
                                password=config.get('database','password') )


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

def send_tweet(twt):
    """
    Method to pushish a pyiem.nws.parser.Tweet object 
    """
    tweet(twt.xtra['channels'], twt.plain, twt.url, twt.extra)

def tweet(channels, msg, url, extras=dict()):
    """
    Method to publish twitter messages
    """    
    if url:
        tweet_step2(channels, msg, extras, url)
    else:
        reallytweet(None, channels, msg, extras)

def reallytweet(jsondata, channels, msg, extras):
    """
    Actually, really publish this time!
    """
    tinyurl = ""
    if jsondata and type(jsondata) == type(""):
        j = json.loads( jsondata )
        if (j.has_key('errorCode') and j['errorCode'] != 0 and 
            j.has_key('errorMessage')):
            if j['errorCode'] != 500:
                email_error(str(j), "Problem with bitly")
        elif j.has_key('results'):
            tinyurl = j['results'][ j['results'].keys()[0] ]['shortUrl']
    tweet_step2(channels, msg, extras, tinyurl)

def tweet_step2(channels, msg, extras, tinyurl=""):
    ''' Step 2 in the twittering '''
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
    factory.addBootstrap("//event/client/basicauth/invaliduser", debug)
    factory.addBootstrap("//event/client/basicauth/authfailed", debug)
    factory.addBootstrap("//event/stream/error", debug)
    factory.addBootstrap(xmlstream.STREAM_END_EVENT, jabber.mydisconnect )

    reactor.connectTCP(config.get('xmpp', 'connecthost'), 5222, factory)

    return jabber

def nounce_uri( uri ):
    '''
    Randomize the URI with meaningless stuff so that twitter does not see my
    usage as spam, sucks to do this
    '''
    if uri.find("?") > 0:
        return '%s&z=%s' % (uri, time.time())
    return '%s?z=%s' % (uri, time.time())
    

def really_really_tweet(tuser, channel, tinyurl, msg, extras):
    """
    Throttled twitter calls
    """
    if tinyurl != "":
        tinyurl = nounce_uri( tinyurl )
        # two spaces and one hashtag (3) , and then 23 for t.co wrapping
        space = 140 - 3 - len(channel) - 23
        twt = "#%s %s %s" % (channel, msg[:space], tinyurl)
    else:
        twt = "#%s %s" % (channel, msg[:118])
    _twitter = twitter.Twitter(consumer=OAUTH_CONSUMER, 
                               token=OAUTH_TOKENS[tuser])
    deffer = _twitter.update( twt, None, extras)
    deffer.addCallback(tb, tuser, channel, twt)
    deffer.addErrback(twitterErrback, tuser, channel, twt)
    deffer.addErrback(log.err)

def tb(x, tuser, channel, twt):
    ''' callback on a good tweet! '''
    log.msg("TWEET User: %s channel: %s TWT: [%s] RES: %s" % (tuser, channel,
                                                              twt, x) )
    
def twitterErrback(error, tuser, channel, twt):
    """
    Errback for twitter calls! 
    """
    error.trap( weberror.Error )
    log.msg("TWEET ERROR User: %s Channel: %s TWT: [%s] RES: %s" % (tuser, 
                                                channel, twt, error) )
    log.msg( error.getErrorMessage() )
    if error.value.status in ["401", "403"]:
        email_error(error.value.response, "Error %s for %s for msg: %s" % (
                                       error.value.status ,tuser, twt))

def debug(data):
    ''' Debug messages '''
    log.msg("DEBUG %s" % (data,))

def rawDataInFn(data):
    ''' Got Data '''
    log.msg("RECV %s" % (data,))

def rawDataOutFn(data):
    ''' Sent data '''
    log.msg("SEND %s" % (data,))

class JabberClient:
    ''' A jabber client class '''

    def __init__(self, myJid):
        ''' Constructor '''
        self.myJid = myJid
        self.xmlstream = None
        self.authenticated = False

    def authd(self, xs):
        ''' called back on authentication '''
        log.msg("Logged into Jabber Chat Server!")

        self.xmlstream = xs
        self.xmlstream.rawDataInFn = rawDataInFn
        self.xmlstream.rawDataOutFn = rawDataOutFn
        presence = domish.Element(('jabber:client','presence'))
        presence.addElement('status').addContent('Online')
        self.xmlstream.send(presence)
        self.authenticated = True

        lc = LoopingCall(self.keepalive)
        lc.start(60)
        self.xmlstream.addObserver(STREAM_END_EVENT, lambda _: lc.stop())

    def keepalive(self):
        ''' white space keepalive, for now '''
        self.xmlstream.send(' ')

    def mydisconnect(self, dummy):
        ''' called back on disconnect ? '''
        log.msg("SETTING authenticated to false!")
        self.authenticated = False

    def send_jmsg(self, jmsg):
        """ Send a pyiem.nws.parser.JabberMessage """
        self.sendMessage(jmsg.plain, jmsg.html, jmsg.xtra)

    def sendMessage(self, body, html, xtra=dict()):
        """ Send a message to the bot for routing! """
        if not self.authenticated:
            log.msg("No Connection, Lets wait and try later...")
            reactor.callLater(3, self.sendMessage, body, html, xtra)
            return
        message = domish.Element(('jabber:client','message'))
        message['to'] = '%s@%s' % (config.get('xmpp', 'iembot'), 
                                   config.get('xmpp', 'domain'))
        message['type'] = 'chat'

        # message.addElement('subject',None,subject)
        message.addElement('body', None, body)
        h = message.addElement('html','http://jabber.org/protocol/xhtml-im')
        b = h.addElement('body', 'http://www.w3.org/1999/xhtml')
        b.addRawXml( html or body )
        # channels is of most important
        x = message.addElement('x', 'nwschat:nwsbot')
        for key in xtra.keys():
            if type(xtra[key]) == type([]):
                x[key] = ",".join(xtra[key])
            else:
                x[key] = xtra[key]
        self.xmlstream.send(message)


defer = _dbpool.runInteraction(load_tokens)
defer.addErrback(email_error)
def stop_pool( dummy ):
    ''' close the temp database pool '''
    _dbpool.close()
defer.addCallback(stop_pool)
