"""
  The CLI report has lots of good data that is hard to find in other products,
  so we take what data we find in this product and overwrite the database
  storage of what we got from the automated observations
"""
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
        deffer = DBPOOL.runInteraction(preprocessor, text)
        deffer.addErrback(common.email_error, text)

def save_data(txn, prod, station, data):
    """ Save atomic data to cli_data table """
    # Use four char here
    station = "%s%s" % (prod.source[0], station)

    if not NT.sts.has_key(station):
        common.email_error("Unknown CLI Station: %s" % (station,),
                           prod.unixtext)

    txn.execute("""
    SELECT product from cli_data where station = %s and valid = %s
    """, (station, data['cli_valid']))
    if txn.rowcount == 1:
        row = txn.fetchone()
        if prod.get_product_id() < row['product']:
            print 'Skip save of %s as previous %s row newer?' % (
                                    prod.get_product_id(), row['product'])
            return
        txn.execute("""DELETE from cli_data WHERE station = %s and valid = %s
        """, (station, data['cli_valid']))

    txn.execute("""INSERT into cli_data(
        station, product, valid, high, high_normal, high_record,
        high_record_years, low, low_normal, low_record, low_record_years,
        precip, precip_month, precip_jan1, precip_jul1, precip_normal,
        precip_record,
        precip_record_years, precip_month_normal, snow, snow_month,
        snow_jun1, snow_jul1, 
        snow_dec1, precip_dec1, precip_dec1_normal, precip_jan1_normal,
        high_time, low_time, snow_record_years, snow_record,
        snow_jun1_normal, snow_jul1_normal, snow_dec1_normal,
        snow_month_normal) 
        VALUES (
        %s, %s, %s, %s, %s, %s,
        %s, %s, %s, %s, %s,
        %s, %s, %s, %s, %s,
        %s,
        %s, %s, %s, %s,
        %s, %s,
        %s, %s, %s, %s,
        %s, %s, %s, %s,
        %s, %s, %s, %s
        )
    """, (station, prod.get_product_id(), data['cli_valid'],
          data['data'].get('temperature_maximum'),
          data['data'].get('temperature_maximum_normal'),
          data['data'].get('temperature_maximum_record'),
          data['data'].get('temperature_maximum_record_years', []),
          data['data'].get('temperature_minimum'),
          data['data'].get('temperature_minimum_normal'),
          data['data'].get('temperature_minimum_record'),
          data['data'].get('temperature_minimum_record_years', []),
          data['data'].get('precip_today'),
          data['data'].get('precip_month'),
          data['data'].get('precip_jan1'), data['data'].get('precip_jul1'),
          data['data'].get('precip_today_normal'),
          data['data'].get('precip_today_record'),
          data['data'].get('precip_today_record_years', []),
          data['data'].get('precip_month_normal'),
          data['data'].get('snow_today'), data['data'].get('snow_month'),
          data['data'].get('snow_jun1'), data['data'].get('snow_jul1'),
          data['data'].get('snow_dec1'), data['data'].get('precip_dec1'),
          data['data'].get('precip_dec1_normal'),
          data['data'].get('precip_jan1_normal'),
          data['data'].get('temperature_maximum_time'),
          data['data'].get('temperature_minimum_time'),
          data['data'].get('snow_today_record_years', []),
          data['data'].get('snow_today_record'),
          data['data'].get('snow_jun1_normal'),
          data['data'].get('snow_jul1_normal'),
          data['data'].get('snow_dec1_normal'),
          data['data'].get('snow_month_normal')
          ))

def send_tweet(prod):
    """ Send the tweet for this prod """

    jres = prod.get_jabbers(    
        common.settings.get('pywwa_product_url', 'pywwa_product_url'))
    for j in jres:
        jabber.sendMessage(j[0], j[1], j[2])

def preprocessor(txn, text):
    """ Protect the realprocessor """
    prod = parser(text)
    if len(prod.data) == 0:
        return
    for data in prod.data:
        realprocessor(txn, prod, data)
    send_tweet(prod)

def realprocessor(txn, prod, data):
    """ Actually do the work """
    # Can't always use the AFOS as the station ID :(
    if len(prod.data) > 1:
        station = None
        for stid in NT.sts.keys():
            if NT.sts[stid]['name'].upper() == data['cli_station']:
                station = stid[1:] # drop first char
                break
        if station is None:
            common.email_error("Unknown CLI Station Text: |%s|" % (
                                                data['cli_station'],), 
                               prod.unixtext)
            return
    else:
        station = prod.afos[3:]
    table = "summary_%s" % (data['cli_valid'].year,)
    txn.execute("""
        SELECT max_tmpf, min_tmpf, pday, pmonth, snow from """+table+""" d
        JOIN stations t on (t.iemid = d.iemid)
        WHERE d.day = %s and t.id = %s and t.network ~* 'ASOS'
        """, (data['cli_valid'], station))
    row = txn.fetchone()
    if row is None:
        print 'No %s rows found for %s on %s' % (table, station, 
                                                 data['cli_valid'])
        save_data(txn, prod, station, data)
        return
    updatesql = []
    logmsg = []
    if data['data'].get('temperature_maximum'):
        climax = data['data']['temperature_maximum']
        if int(climax) != row['max_tmpf']:
            updatesql.append(' max_tmpf = %s' % (climax,))
            logmsg.append('MaxT O:%s N:%s' % (row['max_tmpf'], climax))
    if data['data'].get('temperature_minimum'):
        climin = data['data']['temperature_minimum']
        if int(climin) != row['min_tmpf']:
            updatesql.append(' min_tmpf = %s' % (climin,))
            logmsg.append('MinT O:%s N:%s' % (row['min_tmpf'], climin))
    if data['data'].get('precip_month'):
        val = data['data']['precip_month']
        if val != row['pmonth']:
            updatesql.append(' pmonth = %s' % (val,))
            logmsg.append('PMonth O:%s N:%s' % (row['pmonth'], val))
    if data['data'].get('precip_today'):
        val = data['data']['precip_today']
        if val != row['pday']:
            updatesql.append(' pday = %s' % (val,))
            logmsg.append('PDay O:%s N:%s' % (row['pday'], val))

    if data['data'].get('snow_today'):
        val = data['data']['snow_today']
        if row['snow'] is None or val != row['snow']:
            updatesql.append(' snow = %s' % (val,))
            logmsg.append('Snow O:%s N:%s' % (row['snow'], val))


    if len(updatesql) > 0:
        txn.execute("""UPDATE """+table+""" d SET """+ ','.join(updatesql) +"""
         FROM stations t WHERE t.iemid = d.iemid and d.day = %s and t.id = %s
         and t.network ~* 'ASOS' """, (data['cli_valid'], station))
        log.msg("%s rows for %s (%s) %s" % (txn.rowcount, station,
                                    data['cli_valid'].strftime("%y%m%d"),
                                    ','.join(logmsg)))

    save_data(txn, prod, station, data)


if __name__ == '__main__':
    # Do Stuff
    jabber = common.make_jabber_client('cli_parser')
    ldmbridge.LDMProductFactory(MyProductIngestor())

    reactor.run()
