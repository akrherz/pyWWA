# Common stuff for the pyWWA ingestors...

from twisted.internet import reactor
from twisted.words.xish import domish

import secret, logging

class JabberClient:
    xmlstream = None

    def __init__(self, myJid):
        self.myJid = myJid


    def authd(self,xmlstream):
        logging.info("Logged into Jabber Chat Server!")
        self.xmlstream = xmlstream
        self.xmlstream.rawDataInFn = self.rawDataInFn
        self.xmlstream.rawDataOutFn = self.rawDataOutFn

        presence = domish.Element(('jabber:client','presence'))
        presence.addElement('status').addContent('Online')
        xmlstream.send(presence)

        #xmlstream.addObserver('/message',  self.debug)
        #xmlstream.addObserver('/presence', self.debug)
        #xmlstream.addObserver('/iq',       self.debug)

    def sendMessage(self, body, html=None):
        if (self.xmlstream is None):
            logging.info("xmlstream is None, so lets try again!")
            reactor.callLater(3, self.sendMessage, body, html)
            return
        message = domish.Element(('jabber:client','message'))
        message['to'] = 'iembot@%s' % (secret.chatserver,)
        message['type'] = 'chat'

        # message.addElement('subject',None,subject)
        message.addElement('body',None,body)
        if (html is not None):
            message.addRawXml("<html xmlns='http://jabber.org/protocol/xhtml-im'><body xmlns='http://www.w3.org/1999/xhtml'>"+ html +"</body></html>")
        self.xmlstream.send(message)


    def debug(self, elem):
        logging.info( elem.toXml().encode('utf-8') )

    def rawDataInFn(self, data):
        print 'RECV', unicode(data,'utf-8','ignore').encode('ascii', 'replace')
    def rawDataOutFn(self, data):
        print 'SEND', unicode(data,'utf-8','ignore').encode('ascii', 'replace')
