""" Aviation Product Parser! """
import os
from syslog import LOG_LOCAL2

from twisted.internet import reactor
from twisted.python import syslog
from twisted.python import log
from pyldm import ldmbridge
from pyiem.nws.products.sigmet import parser
import common

syslog.startLogging(prefix='pyWWA/aviation', facility=LOG_LOCAL2)
DBPOOL = common.get_database('postgis')

# Load LOCS table
LOCS = {}

_MYDIR = os.path.dirname(os.path.abspath(__file__))
TABLE_PATH = os.path.normpath(os.path.join(_MYDIR, "..", "tables"))


def load_database(txn):

    txn.execute("""
        SELECT id, name, ST_x(geom) as lon, ST_y(geom) as lat from stations
        WHERE network ~* 'ASOS' or network ~* 'AWOS'
        """)
    for row in txn.fetchall():
        LOCS[row['id']] = row

    for line in open(TABLE_PATH + '/vors.tbl'):
        if len(line) < 70 or line[0] == '!':
            continue
        sid = line[:3]
        lat = float(line[56:60]) / 100.0
        lon = float(line[61:67]) / 100.0
        name = line[16:47].strip()
        LOCS[sid] = {'lat': lat, 'lon': lon, 'name': name}

    # Finally, GEMPAK!
    for line in open(TABLE_PATH + '/pirep_navaids.tbl'):
        if len(line) < 70 or line[0] in ['!', '#']:
            continue
        sid = line[:3]
        lat = float(line[56:60]) / 100.0
        lon = float(line[61:67]) / 100.0
        LOCS[sid] = {'lat': lat, 'lon': lon}


# LDM Ingestor
class MyProductIngestor(ldmbridge.LDMProductReceiver):
    """ I receive products from ldmbridge and process them 1 by 1 :) """

    def connectionLost(self, reason):
        """Stdin was closed"""
        log.msg('connectionLost')
        log.err(reason)
        reactor.callLater(5, self.shutdown)

    def shutdown(self):
        """shutdown soonish"""
        reactor.callWhenRunning(reactor.stop)

    def process_data(self, data):
        """ Process the product """
        try:
            prod = parser(data, nwsli_provider=LOCS)
            # prod.draw()
        except Exception as myexp:
            common.email_error(myexp, data)
            return
        defer = DBPOOL.runInteraction(prod.sql)
        defer.addCallback(final_step, prod)
        defer.addErrback(common.email_error, data)


def final_step(_, prod):
    """send messages"""
    for j in prod.get_jabbers(
            common.SETTINGS.get('pywwa_product_url', 'pywwa_product_url'), ''):
        jabber.send_message(j[0], j[1], j[2])


MESOSITE = common.get_database('mesosite')


def onready(res):
    """Database has loaded"""
    log.msg("onready() called...")
    ldmbridge.LDMProductFactory(MyProductIngestor())
    MESOSITE.close()


df = MESOSITE.runInteraction(load_database)
df.addCallback(onready)
df.addErrback(common.email_error, 'ERROR on load_database')
df.addErrback(log.err)

jabber = common.make_jabber_client("aviation")
reactor.run()
