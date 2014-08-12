""" Aviation Product Parser! """

# Twisted Python imports
from twisted.internet import reactor
from syslog import LOG_LOCAL2
from twisted.python import syslog
syslog.startLogging(prefix='pyWWA/aviation', facility=LOG_LOCAL2)
from twisted.python import log

# pyWWA stuff
from pyldm import ldmbridge
from pyiem.nws.products.sigmet import parser
import common

DBPOOL = common.get_database('postgis')

# Load LOCS table
LOCS = {}
def load_database(txn):
    
    txn.execute("""SELECT id, name, ST_x(geom) as lon, ST_y(geom) as lat from stations 
           WHERE network ~* 'ASOS' or network ~* 'AWOS'""")
    for row in txn:
        LOCS[row['id']] = row

    for line in open('/home/ldm/pyWWA/tables/vors.tbl'):
        if len(line) < 70 or line[0] == '!':
            continue
        sid = line[:3]
        lat = float(line[56:60]) / 100.0
        lon = float(line[61:67]) / 100.0
        name = line[16:47].strip()
        LOCS[sid] = {'lat': lat, 'lon': lon, 'name': name}
    
    # Finally, GEMPAK!
    for line in open('/home/ldm/pyWWA/tables/pirep_navaids.tbl'):
        sid = line[:3]
        lat = float(line[56:60]) / 100.0
        lon = float(line[61:67]) / 100.0
        LOCS[sid] = {'lat': lat, 'lon': lon}


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
            prod = parser(buf, nwsli_provider=LOCS)
            #prod.draw()
        except Exception, myexp:
            common.email_error(myexp, buf)
            return
        defer = DBPOOL.runInteraction(prod.sql)
        defer.addCallback(final_step, prod)
        defer.addErrback(common.email_error, buf)
            
def final_step(_, prod):
    """

    """
    for j in prod.get_jabbers(
            common.settings.get('pywwa_product_url', 'pywwa_product_url'), ''):
        jabber.sendMessage(j[0], j[1], j[2])

MESOSITE = common.get_database('mesosite')

def onready(res):
    log.msg("onready() called...")
    ldmbridge.LDMProductFactory( MyProductIngestor() )
    MESOSITE.close()

df = MESOSITE.runInteraction(load_database)
df.addCallback(onready)
df.addErrback( log.err )

jabber = common.make_jabber_client("aviation")
reactor.run()