"""SPC Geo Products Parser!"""

# 3rd Party
from twisted.internet import reactor
from pyldm import ldmbridge
from pyiem.util import LOG
from pyiem.nws.products.spcpts import parser

# Local
from pywwa import common
from pywwa.xmpp import make_jabber_client

DBPOOL = common.get_database("postgis")
WAITFOR = 20
JABBER = make_jabber_client()


# LDM Ingestor
class MyProductIngestor(ldmbridge.LDMProductReceiver):
    """ I receive products from ldmbridge and process them 1 by 1 :) """

    def connectionLost(self, reason):
        """shutdown"""
        common.shutdown()

    def process_data(self, data):
        """ Process the product """
        df = DBPOOL.runInteraction(real_parser, data)
        df.addErrback(common.email_error, data)


def real_parser(txn, buf):
    """Actually process"""
    spc = parser(buf)
    # spc.draw_outlooks()
    spc.compute_wfos(txn)
    if common.dbwrite_enabled():
        spc.sql(txn)
    jmsgs = spc.get_jabbers("")
    for (txt, html, xtra) in jmsgs:
        JABBER.send_message(txt, html, xtra)
    LOG.info(
        "Sent %s messages for product %s", len(jmsgs), spc.get_product_id()
    )


if __name__ == "__main__":
    ldmbridge.LDMProductFactory(MyProductIngestor())
    reactor.run()  # @UndefinedVariable
