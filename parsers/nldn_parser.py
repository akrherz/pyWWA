""" NLDN """
from io import BytesIO

from twisted.internet import reactor
from pyldm import ldmbridge
from pyiem.nws.products.nldn import parser
import common

DBPOOL = common.get_database('postgis')


class myProductIngestor(ldmbridge.LDMProductReceiver):
    """My hacky ingest"""
    product_end = b'NLDN'

    def process_data(self, data):
        """Actual ingestor"""
        if data == b"":
            return
        real_process(data)
        try:
            real_process(data)
        except Exception as myexp:
            common.email_error(myexp, data)

    def connectionLost(self, reason):
        """
        Called when ldm closes the pipe
        """
        print('connectionLost')
        print(reason)
        reactor.callLater(5, self.shutdown)

    def shutdown(self):
        """shutdown please"""
        reactor.callWhenRunning(reactor.stop)


def real_process(buf):
    """ The real processor of the raw data, fun! """
    np = parser(BytesIO(b"NLDN" + buf))
    DBPOOL.runInteraction(np.sql)


def main():
    """Go Main"""
    ldmbridge.LDMProductFactory(myProductIngestor(isbinary=True))
    reactor.run()  # @UndefinedVariable


if __name__ == '__main__':
    main()
