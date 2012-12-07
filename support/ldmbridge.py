
from twisted.internet import stdio
from twisted.protocols import basic
from twisted.internet import reactor
from twisted.python import log

class LDMProductReceiver(basic.LineReceiver):
    delimiter = '\n'
    product_start = '\001'
    product_end = '\r\r\n\003'

    def __init__(self):
        self.productBuffer = ""
        self.setRawMode()
        self.cbFunc = self.process_data

    def rawDataReceived(self, data):
        """ callback for when raw data is received on the stdin buffer, this 
        could be a partial product or lots of products """
        # See if we have anything left over from previous iteration
        if self.productBuffer != "":
            data = self.productBuffer + data
        
        tokens = data.split(self.product_end)
        # If length tokens is 1, then we did not find the splitter
        if len(tokens) == 1:
            #log.msg("Token not found, len data %s" % (len(data),))
            self.productBuffer = data
            return

        # Everything up until the last one can always go...        
        for token in tokens[:-1]:
            #log.msg("ldmbridge cb product size: %s" % (len(token),))
            reactor.callLater(0, self.cbFunc, token)
        # We have some cruft left over!
        if tokens[-1] != "":
            self.productBuffer = tokens[-1]
        else:
            self.productBuffer = ""
                    
    def connectionLost(self, reason):
        raise NotImplementedError

    def process_data(self, data):
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
        if fd == 'read':
            self.connectionLost(reason)
        else:
            self._writeConnectionLost(reason)

