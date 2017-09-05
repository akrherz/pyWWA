""" NLDN """

# Twisted Python imports
from syslog import LOG_LOCAL2
from twisted.python import syslog
syslog.startLogging(prefix='pyWWA/nldn_parser', facility=LOG_LOCAL2)

# Twisted Python imports
from twisted.internet import reactor

# pyWWA stuff
from pyldm import ldmbridge
from pyiem.nws.products.nldn import parser
import common
import cStringIO

DBPOOL = common.get_database('postgis')


class myProductIngestor(ldmbridge.LDMProductReceiver):
    product_end = 'NLDN'

    def process_data(self, buf):
        """
        Actual ingestor
        """
        if buf == "":
            return
        try:
            real_process(buf)
        except Exception, myexp:
            common.email_error(myexp, buf)

    def connectionLost(self, reason):
        """
        Called when ldm closes the pipe
        """
        print 'connectionLost', reason
        reactor.callLater(5, self.shutdown)

    def shutdown(self):
        reactor.callWhenRunning(reactor.stop)


def real_process(buf):
    """ The real processor of the raw data, fun! """
    np = parser(cStringIO.StringIO("NLDN" + buf))
    DBPOOL.runInteraction(np.sql)


def main():
    """Go Main"""
    _ = ldmbridge.LDMProductFactory(myProductIngestor(isbinary=True))
    reactor.run()  # @UndefinedVariable


if __name__ == '__main__':
    main()
