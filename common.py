# Common stuff for the pyWWA ingestors...

from twisted.internet import reactor
from twisted.words.xish import domish
from twisted.python import log
from twisted.mail import smtp
from twisted.web import client
from twisted.words.xish.xmlstream import STREAM_END_EVENT
from twisted.internet.task import LoopingCall

import secret
from twittytwister import twitter
import urllib
import simplejson

WFOS = [ 'APX', 'DLH', 'GRR',
 'FGZ', 'TAE', 'BMX', 'PSR', 'TWC', 'SGX', 'HNX', 'EKA', 'LOX', 'GLD', 'PHI',
 'TBW', 'MFL', 'IWX', 'IND', 'ILN', 'EAX', 'ICT', 'DDC', 'DTX', 'FGF', 'ARX',
 'FSD', 'BIS', 'GID', 'JAX', 'OAX', 'CYS', 'ILX', 'LSX', 'MTR', 'STO', 'BOU',
 'LOT', 'TOP', 'SGF', 'CLE', 'MPX', 'RAH', 'LBF', 'EPZ', 'LKN', 'TSA', 'CTP',
 'SJU', 'UNR', 'SEW', 'MKX', 'AFC', 'AJK', 'AFG', 'OUN', 'MFR', 'PDT', 'MOB',
 'RLX', 'GRB', 'MQT', 'ABR', 'OTX', 'PQR', 'BTV', 'LWX', 'FFC', 'GJT', 'PUB',
 'GGW', 'ABQ', 'CAE', 'CHS', 'GSP', 'PAH', 'DVN', 'GUM', 'JSJ', 'RNK', 'AKQ',
 'SLC', 'AMA', 'LUB', 'FWD', 'MAF', 'SJT', 'MHX', 'ILM', 'TFX', 'MSO', 'EWX',
 'HGX', 'CRP', 'BRO', 'MRX', 'OHX', 'MEG', 'PBZ', 'BUF', 'ALY', 'BGM', 'OKX',
 'REV', 'VEF', 'RIW', 'BYZ', 'JAN', 'CAR', 'GYX', 'BOX', 'SHV', 'LCH', 'LIX',
 'LMK', 'JKL', 'BOI', 'PIH', 'KEY', 'HFO', 'MLB', 'LZK', 'HUN', 'DMX',
 'SPC', 'NHC']


class JabberClient:

    def __init__(self, myJid):
        self.myJid = myJid
        self.xmlstream = None
        self.authenticated = False
        self.twit = {}
        for wfo in WFOS:
            self.twit[wfo] = twitter.Twitter('iembot_%s' % (wfo.lower(),) , 
                secret.twitter_pass)

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

    def sendMessage(self, body, html=None, to_user=secret.iembot_user):
        if (not self.authenticated):
            log.msg("No Connection, Lets wait and try later...")
            reactor.callLater(3, self.sendMessage, body, html, to_user)
            return
        message = domish.Element(('jabber:client','message'))
        message['to'] = '%s@%s' % (to_user, secret.chatserver)
        message['type'] = 'chat'

        # message.addElement('subject',None,subject)
        message.addElement('body',None,body)
        h = message.addElement('html','http://jabber.org/protocol/xhtml-im')
        b = h.addElement('body', 'http://www.w3.org/1999/xhtml')
        b.addRawXml( html or body )
        self.xmlstream.send(message)

        url = self.findurl(body)
        if url:
            #url = urllib.quote_plus(url)
            url = url.replace("&amp;", "&").replace("#","%23")
            #http://tinyurl.com/api-create.php?url="+url
            client.getPage("http://api.bit.ly/shorten?version=2.0.1&longUrl=%s&login=iembot&apiKey=%s" % (url, secret.bitly_key) ).addCallback(self.tweet, " ".join(body.split(" ")[:-1]))
        else:
            self.tweet(None, body)

    def findurl(self, message):
        """Extract URL from message"""
        tokens = message.split(" ")
        if len(tokens) == 0:
            return None
        if tokens[-1][:4] != "http":
            return None
        return tokens[-1]

    def tweet(self, json, body):
        if json:
            j = simplejson.loads( json )
            tinyurl = j['results'][ j['results'].keys()[0] ]['shortUrl']
        else:
            tinyurl = ""
        wfo = body[:3].upper()
        if not self.twit.has_key(wfo):
            print "Unknown Twitter Channel, %s" % (wfo,)
            return
        if not secret.DISARM:
            self.twit[wfo].update("#%s %s" % (body[:112], tinyurl) ).addCallback(self.tb)
        else:
            print "TWEET HERE!"

    def tb(self,x):
        print 'TWITTER RESPONSE ', x

    def debug(self, elem):
        log.msg( elem.toXml().encode('utf-8') )

    def rawDataInFn(self, data):
        print 'RECV', unicode(data,'utf-8','ignore').encode('ascii', 'replace')
    def rawDataOutFn(self, data):
        if (data == ' '): return 
        print 'SEND', unicode(data,'utf-8','ignore').encode('ascii', 'replace')
