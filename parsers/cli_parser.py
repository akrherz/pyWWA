"""
  The CLI report has lots of good data that is hard to find in other products,
  so we take what data we find in this product and overwrite the database
  storage of what we got from the automated observations
"""
# http://bugs.python.org/issue7980
import datetime
datetime.datetime.strptime('2013', '%Y')

from pyiem.nws.products import parser
from pyldm import ldmbridge
from pyiem.network import Table as NetworkTable
import common

from twisted.internet import reactor
from syslog import LOG_LOCAL2
from twisted.python import syslog
syslog.startLogging(prefix='pyWWA/cli_parser', facility=LOG_LOCAL2)
from twisted.python import log

DBPOOL = common.get_database('iem', cp_max=1)
NT = NetworkTable("NWSCLI")

# LDM Ingestor
class MyProductIngestor(ldmbridge.LDMProductReceiver):
    """ I receive products from ldmbridge and process them 1 by 1 :) """

    def connectionLost(self, reason):
        """ Connection was lost! """
        log.err(reason)
        reactor.callLater(7, reactor.callWhenRunning, reactor.stop)

    def process_data(self, text):
        """ Process the product """
        deffer = DBPOOL.runInteraction(realparser, text)
        deffer.addErrback(common.email_error, text)

def save_data(txn, prod):
    """ Save atomic data to cli_data table """
    # hopefully this prevents issues with ID conflicts and makes it easier
    # to match with ASOS sites
    station = "%s%s" % (prod.source[0], prod.afos[3:])

    if not NT.sts.has_key(station):
        common.email_error("Unknown CLI Station: %s" % (station,),
                           prod.unixtext)

    txn.execute("""
    SELECT product from cli_data where station = %s and valid = %s
    """, (station, prod.cli_valid))
    if txn.rowcount == 1:
        row = txn.fetchone()
        if prod.get_product_id() < row['product']:
            print 'Skip save of %s as previous %s row newer?' % (
                                    prod.get_product_id(), row['product'])
            return
        txn.execute("""DELETE from cli_data WHERE station = %s and valid = %s
        """, (station, prod.cli_valid))

    txn.execute("""INSERT into cli_data(
        station, product, valid, high, high_normal, high_record,
        high_record_years, low, low_normal, low_record, low_record_years,
        precip, precip_month, precip_jan1, precip_jul1, precip_normal,
        precip_record,
        precip_record_years, precip_month_normal, snow, snow_month,
        snow_jun1, snow_jul1, 
        snow_dec1, precip_dec1, precip_dec1_normal, precip_jan1_normal,
        high_time, low_time, snow_record_years, snow_record) 
        VALUES (
        %s, %s, %s, %s, %s, %s,
        %s, %s, %s, %s, %s,
        %s, %s, %s, %s, %s,
        %s,
        %s, %s, %s, %s,
        %s, %s,
        %s, %s, %s, %s,
        %s, %s, %s, %s
        )
    """, (station, prod.get_product_id(), prod.cli_valid,
          prod.data.get('temperature_maximum'),
          prod.data.get('temperature_maximum_normal'),
          prod.data.get('temperature_maximum_record'),
          prod.data.get('temperature_maximum_record_years', []),
          prod.data.get('temperature_minimum'),
          prod.data.get('temperature_minimum_normal'),
          prod.data.get('temperature_minimum_record'),
          prod.data.get('temperature_minimum_record_years', []),
          prod.data.get('precip_today'),
          prod.data.get('precip_month'),
          prod.data.get('precip_jan1'), prod.data.get('precip_jul1'),
          prod.data.get('precip_today_normal'),
          prod.data.get('precip_today_record'),
          prod.data.get('precip_today_record_years', []),
          prod.data.get('precip_month_normal'),
          prod.data.get('snow_today'), prod.data.get('snow_month'),
          prod.data.get('snow_jun1'), prod.data.get('snow_jul1'),
          prod.data.get('snow_dec1'), prod.data.get('precip_dec1'),
          prod.data.get('precip_dec1_normal'),
          prod.data.get('precip_jan1_normal'),
          prod.data.get('temperature_maximum_time'),
          prod.data.get('temperature_minimum_time'),
          prod.data.get('snow_today_record_years', []),
          prod.data.get('snow_today_record')
          ))

def send_tweet(prod):
    """ Send the tweet for this prod """

    jres = prod.get_jabbers(    
        common.settings.get('pywwa_product_url', 'pywwa_product_url'))
    jabber.sendMessage(jres[0], jres[1], jres[2])



def realparser(txn, text):
    """ Actually do the work """
    prod = parser(text)
    if prod.data is None or prod.cli_valid is None:
        return

    station = prod.afos[3:]
    table = "summary_%s" % (prod.cli_valid.year,)
    txn.execute("""
        SELECT max_tmpf, min_tmpf, pday, pmonth, snow from """+table+""" d
        JOIN stations t on (t.iemid = d.iemid)
        WHERE d.day = %s and t.id = %s and t.network ~* 'ASOS'
        """, (prod.cli_valid, station))
    row = txn.fetchone()
    if row is None:
        print 'No %s rows found for %s on %s' % (table, station, prod.cli_valid)
        save_data(txn, prod)
        send_tweet(prod)
        return
    updatesql = []
    logmsg = []
    if prod.data.get('temperature_maximum'):
        climax = prod.data['temperature_maximum']
        if int(climax) != row['max_tmpf']:
            updatesql.append(' max_tmpf = %s' % (climax,))
            logmsg.append('MaxT O:%s N:%s' % (row['max_tmpf'], climax))
    if prod.data.get('temperature_minimum'):
        climin = prod.data['temperature_minimum']
        if int(climin) != row['min_tmpf']:
            updatesql.append(' min_tmpf = %s' % (climin,))
            logmsg.append('MinT O:%s N:%s' % (row['min_tmpf'], climin))
    if prod.data.get('precip_month'):
        val = prod.data['precip_month']
        if val != row['pmonth']:
            updatesql.append(' pmonth = %s' % (val,))
            logmsg.append('PMonth O:%s N:%s' % (row['pmonth'], val))
    if prod.data.get('precip_today'):
        val = prod.data['precip_today']
        if val != row['pday']:
            updatesql.append(' pday = %s' % (val,))
            logmsg.append('PDay O:%s N:%s' % (row['pday'], val))

    if prod.data.get('snow_today'):
        val = prod.data['snow_today']
        if val != row['snow']:
            updatesql.append(' snow = %s' % (val,))
            logmsg.append('Snow O:%s N:%s' % (row['pday'], val))


    if len(updatesql) > 0:
        txn.execute("""UPDATE """+table+""" d SET """+ ','.join(updatesql) +"""
         FROM stations t WHERE t.iemid = d.iemid and d.day = %s and t.id = %s
         and t.network ~* 'ASOS' """, (prod.cli_valid, station))
        log.msg("%s rows for %s (%s) %s" % (txn.rowcount, station,
                                    prod.cli_valid.strftime("%y%m%d"),
                                    ','.join(logmsg)))

    save_data(txn, prod)
    send_tweet(prod)


if __name__ == '__main__':
    # Do Stuff
    jabber = common.make_jabber_client('cli_parser')
    ldmbridge.LDMProductFactory(MyProductIngestor())

    reactor.run()
