
# Python imports
import sys, re

from twisted.internet import stdio, error
from twisted.protocols import basic
from twisted.internet import reactor

class LDMProductReceiver(basic.LineReceiver):
    delimiter = '\n'
    productDelimiter = '\003'

    def __init__(self):
        self.productBuffer = ""
        self.setRawMode()

    def rawDataReceived(self, data):
        tokens = re.split(self.productDelimiter, data)
        if (len(tokens) == 1):
            self.productBuffer += data
        else:
            reactor.callLater(0,self.processData,self.productBuffer + tokens[0])
            self.productBuffer = tokens[-1]
            for token in tokens[1:-1]:
                reactor.callLater(0, self.processData, token)
        del tokens           

    def connectionLost(self, reason):
        raise NotImplementedError

    def processData(self, data):
        raise NotImplementedError

class LDMProductFactory( stdio.StandardIO ):

    def __init__(self, protocol):
        self.protocol = protocol
        stdio.StandardIO.__init__(self, protocol)

#    def connectionLost(self, reason):
#        self.protocol.connectionLost(reason)

    def childConnectionLost(self, fd, reason):
        if self.disconnected:
            return
        print 'childConnectionLost', fd
        if fd == 'read':
            self.connectionLost(reason)
        else:
            self._writeConnectionLost(reason)

