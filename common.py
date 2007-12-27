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
        presence = domish.Element(('jabber:client','presence'))
        xmlstream.send(presence)

        xmlstream.addObserver('/message',  self.debug)
        xmlstream.addObserver('/presence', self.debug)
        xmlstream.addObserver('/iq',       self.debug)

    def sendMessage(self, body, html=None):
        while (self.xmlstream is None):
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
        self.debug( message )
        self.xmlstream.send(message)


    def debug(self, elem):
        logging.info( elem.toXml().encode('utf-8') )
        logging.info("="*20 )
