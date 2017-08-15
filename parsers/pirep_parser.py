""" PIREP parser! """
from syslog import LOG_LOCAL2
import datetime
import os

from twisted.python import syslog
from twisted.internet import reactor
from twisted.python import log
import common
from pyldm import ldmbridge
from pyiem.nws.products.pirep import parser as pirepparser

syslog.startLogging(prefix='pyWWA/pirep_parser', facility=LOG_LOCAL2)

TABLESDIR = os.path.join(os.path.dirname(__file__), "../tables")

PIREPS = {}
DBPOOL = common.get_database("postgis")
JABBER = common.make_jabber_client('pirep')
# Load LOCS table
LOCS = {}


def cleandb():
    """ To keep LSRDB from growing too big, we clean it out
        Lets hold 1 days of data!
    """
    thres = datetime.datetime.utcnow() - datetime.timedelta(hours=24*1)
    init_size = len(PIREPS.keys())
    for key in PIREPS:
        if PIREPS[key] < thres:
            del PIREPS[key]

    fin_size = len(PIREPS.keys())
    log.msg("cleandb() init_size: %s final_size: %s" % (init_size, fin_size))

    # Call Again in 30 minutes
    reactor.callLater(60*30, cleandb)  # @UndefinedVariable


def load_locs(txn):
    """Build locations table"""
    log.msg("load_locs() called...")
    txn.execute("""SELECT id, name, st_x(geom) as lon, st_y(geom) as lat
        from stations WHERE network ~* 'ASOS' or network ~* 'AWOS'""")
    for row in txn:
        LOCS[row['id']] = {'id': row['id'], 'name': row['name'],
                           'lon': row['lon'], 'lat': row['lat']}

    for line in open(TABLESDIR+'/faa_apt.tbl'):
        if len(line) < 70 or line[0] == '!':
            continue
        sid = line[:4].strip()
        lat = float(line[56:60]) / 100.0
        lon = float(line[61:67]) / 100.0
        name = line[16:47].strip()
        if sid not in LOCS:
            LOCS[sid] = {'lat': lat, 'lon': lon, 'name': name}

    for line in open(TABLESDIR+'/vors.tbl'):
        if len(line) < 70 or line[0] == '!':
            continue
        sid = line[:3]
        lat = float(line[56:60]) / 100.0
        lon = float(line[61:67]) / 100.0
        name = line[16:47].strip()
        if sid not in LOCS:
            LOCS[sid] = {'lat': lat, 'lon': lon, 'name': name}

    # Finally, GEMPAK!
    for line in open(TABLESDIR+'/pirep_navaids.tbl'):
        if len(line) < 60 or line[0] in ['!', '#']:
            continue
        sid = line[:4].strip()
        lat = float(line[56:60]) / 100.0
        lon = float(line[61:67]) / 100.0
        if sid not in LOCS:
            LOCS[sid] = {'lat': lat, 'lon': lon}

    log.msg("... %s locations loaded" % (len(LOCS),))


# LDM Ingestor
class MyProductIngestor(ldmbridge.LDMProductReceiver):
    """ I receive products from ldmbridge and process them 1 by 1 :) """

    def connectionLost(self, reason):
        """Connection was lost for some reason"""
        log.msg('connectionLost')
        log.err(reason)
        reactor.callLater(5, self.shutdown)  # @UndefinedVariable

    def shutdown(self):
        """shutdown"""
        reactor.callWhenRunning(reactor.stop)  # @UndefinedVariable

    def process_data(self, data):
        """ Process the product """
        defer = DBPOOL.runInteraction(real_parser, data)
        defer.addErrback(common.email_error, data)
        defer.addErrback(log.err)


def real_parser(txn, buf):
    """
    I'm gonna do the heavy lifting here
    """
    prod = pirepparser(buf, nwsli_provider=LOCS)
    prod.assign_cwsu(txn)
    for report in prod.reports:
        if report.text in PIREPS:
            report.is_duplicate = True
        PIREPS[report.text] = datetime.datetime.utcnow()

    j = prod.get_jabbers()
    if prod.warnings:
        common.email_error("\n".join(prod.warnings), buf)
    for msg in j:
        JABBER.send_message(msg[0], msg[1], msg[2])

    prod.sql(txn)


def ready(_bogus):
    """We are ready to ingest"""
    reactor.callLater(20, cleandb)  # @UndefinedVariable
    ldmbridge.LDMProductFactory(MyProductIngestor())


def shutdown(err):
    """a shutown method"""
    log.msg(err)
    reactor.stop()  # @UndefinedVariable


if __name__ == '__main__':
    df = DBPOOL.runInteraction(load_locs)
    df.addCallback(ready)
    df.addErrback(shutdown)

    reactor.run()  # @UndefinedVariable
