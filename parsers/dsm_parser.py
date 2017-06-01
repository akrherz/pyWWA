""" ASOS Daily Summary Message Parser ingestor """

# Twisted Python imports
from syslog import LOG_LOCAL2
from twisted.python import syslog
syslog.startLogging(prefix='pyWWA/dsm_parser', facility=LOG_LOCAL2)
from twisted.python import log

# Twisted Python imports
from twisted.internet import reactor

# Standard Python modules
import re

# Python 3rd Party Add-Ons
import datetime

# pyWWA stuff
from pyldm import ldmbridge
import common

DBPOOL = common.get_database("iem", cp_max=1)


class MyProductIngestor(ldmbridge.LDMProductReceiver):
    """ I receive products from ldmbridge and process them 1 by 1 :) """

    def connectionLost(self, reason):
        log.msg('connectionLost')
        log.err(reason)
        reactor.callLater(5, self.shutdown)

    def shutdown(self):
        reactor.callWhenRunning(reactor.stop)

    def process_data(self, buf):
        """ Process the product """
        try:
            real_parser(buf)
        except Exception, myexp:
            common.email_error(myexp, buf)


def real_parser(buf):
    # KAMW DS 19/07 761539/ 540532// 76/ 54//0062028/00/00/00/00/00/00/00/
    # 00/00/00/00/00/00/00/00/00/00/00/00/00/00/00/00/00/00/25/12091717/
    # 14131452=
    # Split lines
    raw = buf.replace("\015\015\012", "\n")
    lines = raw.split("\n")
    if len(lines[3]) < 10:
        meat = ("".join(lines[4:])).split("=")
    else:
        meat = ("".join(lines[3:])).split("=")
    for data in meat:
        if data == "":
            continue
        process_dsm(data)

PARSER_RE = re.compile("""^(?P<id>[A-Z][A-Z0-9]{3})\s+
   DS\s+
   (COR\s)?
   ([0-9]{4}\s)?
   (?P<day>\d\d)/(?P<month>\d\d)\s?
   ((?P<highmiss>M)|((?P<high>(-?\d+))(?P<hightime>[0-9]{4})))/\s?
   ((?P<lowmiss>M)|((?P<low>(-?\d+))(?P<lowtime>[0-9]{4})))//\s?
   (?P<coophigh>(-?\d+|M))/\s?
   (?P<cooplow>(-?\d+|M))//
   (?P<minslp>M|[\-0-9]{3,4})(?P<slptime>[0-9]{4})?/
   (?P<precip>T|M|[0-9]{,4})/
 (?P<p01>T|M|\-|[0-9]{,4})/(?P<p02>T|M|\-|[0-9]{,4})/(?P<p03>T|M|\-|[0-9]{,4})/
 (?P<p04>T|M|\-|[0-9]{,4})/(?P<p05>T|M|\-|[0-9]{,4})/(?P<p06>T|M|\-|[0-9]{,4})/
 (?P<p07>T|M|\-|[0-9]{,4})/(?P<p08>T|M|\-|[0-9]{,4})/(?P<p09>T|M|\-|[0-9]{,4})/
 (?P<p10>T|M|\-|[0-9]{,4})/(?P<p11>T|M|\-|[0-9]{,4})/(?P<p12>T|M|\-|[0-9]{,4})/
 (?P<p13>T|M|\-|[0-9]{,4})/(?P<p14>T|M|\-|[0-9]{,4})/(?P<p15>T|M|\-|[0-9]{,4})/
 (?P<p16>T|M|\-|[0-9]{,4})/(?P<p17>T|M|\-|[0-9]{,4})/(?P<p18>T|M|\-|[0-9]{,4})/
 (?P<p19>T|M|\-|[0-9]{,4})/(?P<p20>T|M|\-|[0-9]{,4})/(?P<p21>T|M|\-|[0-9]{,4})/
 (?P<p22>T|M|\-|[0-9]{,4})/(?P<p23>T|M|\-|[0-9]{,4})/(?P<p24>T|M|\-|[0-9]{,4})/
   (?P<avg_sped>[0-9]{3})?/?
   (?P<drct_sped_max>[0-9]{2})?(?P<sped_max>[0-9]{2})?(?P<time_sped_max>[0-9]{4})?/?
   (?P<drct_gust_max>[0-9]{2})?(?P<sped_gust_max>[0-9]{2})?(?P<time_gust_max>[0-9]{4})?
""", re.VERBOSE)


def process_dsm(data):
    m = PARSER_RE.match(data)
    if m is None:
        log.msg("FAIL ||%s||" % (data, ))
        common.email_error("DSM RE Match Failure", data)
        return
    d = m.groupdict()
    # Only parse United States Sites
    if d['id'][0] != "K":
        return
    # Figure out the timestamp
    now = datetime.datetime.now()
    if d['day'] == '00' or d['month'] == '00':
        common.email_error("ERROR: Invalid Timestamp", data)
        return
    ts = now.replace(day=int(d['day']), month=int(d['month']))
    if ts.month == 12 and now.month == 1:
        ts -= datetime.timedelta(days=360)
        ts = ts.replace(day=int(d['day']))
    updater = []
    if d['high'] is not None and d['high'] != "M":
        updater.append("max_tmpf = %s" % (d['high'],))
    if d['low'] is not None and d['low'] != "M":
        updater.append("min_tmpf = %s" % (d['low'],))

    if d['precip'] is not None and d['precip'] not in ["M", "T"]:
        updater.append("pday = %s" % (float(d['precip']) / 100.0,))
    elif d['precip'] == "T":
        updater.append("pday = 0.0001")

    sql = """
        UPDATE summary_%s s SET %s FROM stations t WHERE t.iemid = s.iemid
        and t.id = '%s' and day = '%s'
        """ % (ts.year, " , ".join(updater), d['id'][1:],
               ts.strftime("%Y-%m-%d"))
    log.msg("%s %s H:%s L:%s P:%s" % (d['id'], ts.strftime("%Y-%m-%d"),
                                      d['high'], d['low'], d['precip']))
    if len(updater) > 0:
        defer = DBPOOL.runOperation(sql)
        defer.addErrback(common.email_error, sql)

if __name__ == '__main__':
    ldm = ldmbridge.LDMProductFactory(MyProductIngestor())
    reactor.run()
