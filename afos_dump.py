""" Twisted Way to dump data to the database """

# Twisted Python imports
from twisted.python import log, logfile
log.FileLogObserver.timeFormat = "%Y/%m/%d %H:%M:%S %Z"
log.startLogging(logfile.DailyLogFile('afos_dump.log','logs/'))

from twisted.internet import reactor

import os
import sys

import ConfigParser
config = ConfigParser.ConfigParser()
config.read(os.path.join(os.path.dirname(__file__), 'cfg.ini'))

from pyldm import ldmbridge
from pyiem.nws import product
import common
import datetime
import pytz

DBPOOL = common.get_database('afos')

# LDM Ingestor
class MyProductIngestor(ldmbridge.LDMProductReceiver):
    """ I receive products from ldmbridge and process them 1 by 1 :) """

    def connectionLost(self, reason):
        log.msg('connectionLost')
        log.err( reason )
        reactor.callLater(5, self.shutdown)

    def shutdown(self):
        reactor.callWhenRunning(reactor.stop)


    def process_data(self, buf):
        """ Process the product """
        try:
            real_parser(buf)
        except Exception, myexp:
            common.email_error(myexp, buf)

class ParseError(Exception):
    """ general exception """
    pass

def real_parser(buf):
    """ Actually do something with the buffer, please """
    if buf.strip() == "":
        return
    utcnow = datetime.datetime.utcnow()
    utcnow = utcnow.replace(tzinfo=pytz.timezone("UTC"))
    
    nws = product.TextProduct( buf)

    if (utcnow - nws.valid).days > 180 or (utcnow - nws.valid).days < -180:
        common.email_error("Very Latent Product! %s" % (nws.valid,), nws.text)
        return
    
    if nws.valid.month > 6:
        table = "products_%s_0712" % (nws.valid.year,)
    else:
        table = "products_%s_0106" % (nws.valid.year,)
    
    if nws.afos is None:
        if MANUAL:
            return
        raise ParseError("TextProduct.afos is null")
        
    df = DBPOOL.runInteraction(run_db, table, nws)
    df.addErrback( common.email_error, buf)
    df.addErrback( log.err )

def run_db(txn, table, nws):
    """ Run the database transaction """
    if MANUAL:
        txn.execute("""SELECT * from """+table+""" WHERE
        pil = %s and entered = %s and source = %s and wmo = %s
        """, (nws.afos.strip(), nws.valid, nws.source, nws.wmo))
        if txn.rowcount == 1:
            log.msg("Duplicate: %s" % (nws.get_product_id(),))
            return
    txn.execute("""INSERT into """+table+"""(pil, data, entered,
        source, wmo) VALUES(%s,%s,%s,%s,%s)""",  (nws.afos.strip(), nws.text, 
                             nws.valid,
                             nws.source, nws.wmo))

if __name__ == '__main__':
    # Go
    MANUAL = False
    if len(sys.argv) == 2 and sys.argv[1] == 'manual':
        MANUAL = True
    ldm = ldmbridge.LDMProductFactory( MyProductIngestor() )
    reactor.run()
