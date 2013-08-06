"""
  Ingest VTEC information
"""
import os

from pyiem.nws.product import parser
from pyldm import ldmbridge
import common

from twisted.internet import reactor
from twisted.python import log, logfile
log.FileLogObserver.timeFormat = "%Y/%m/%d %H:%M:%S %Z"
log.startLogging( logfile.DailyLogFile('cli_parser.log','logs'))
from twisted.enterprise import adbapi

import ConfigParser
config = ConfigParser.ConfigParser()
config.read(os.path.join(os.path.dirname(__file__), 'cfg.ini'))

DBPOOL = adbapi.ConnectionPool("twistedpg", database="iem", 
                                cp_reconnect=True,
                                host=config.get('database','host'), 
                                user=config.get('database','user'),
                                password=config.get('database','password')) 

# LDM Ingestor
class MyProductIngestor(ldmbridge.LDMProductReceiver):
    """ I receive products from ldmbridge and process them 1 by 1 :) """

    def connectionLost(self, reason):
        reactor.callLater(7, self.shutdown)

    def shutdown(self):
        reactor.callWhenRunning(reactor.stop)

    def process_data(self, text):
        """ Process the product """
        deffer = DBPOOL.runInteraction(realparser, text)
        deffer.addErrback(common.email_error, text)
        

def realparser(txn, text):
    ''' Actually do the work '''
    prod = parser( text )
    if prod.data is None or prod.cli_valid is None:
        return
    
    station = prod.afos[3:]
    table = "summary_%s" % (prod.cli_valid.year,)
    txn.execute("""SELECT max_tmpf, min_tmpf, pday, pmonth from """+table+""" d 
    JOIN stations t
     on (t.iemid = d.iemid) WHERE d.day = %s and t.id = %s and t.network ~* 'ASOS'
        """, (prod.cli_valid, station))
    row = txn.fetchone()
    if row is None:
        print 'No %s rows found for %s on %s' % (table, station, prod.cli_valid)
        return
    updatesql = []
    logmsg = []
    if prod.data.get('temperature_maximum'):
        climax = prod.data['temperature_maximum']
        if int(climax) != row['max_tmpf']:
            updatesql.append(' max_tmpf = %s' % (climax,))
            logmsg.append( 'MaxT O:%s N:%s' % (row['max_tmpf'], climax))
    if prod.data.get('temperature_minimum'):
        climin = prod.data['temperature_minimum']
        if int(climin) != row['min_tmpf']:
            updatesql.append(' min_tmpf = %s' % (climin,)) 
            logmsg.append( 'MinT O:%s N:%s' % (row['min_tmpf'], climin))
    if prod.data.get('precip_month'):
        val = prod.data['precip_month']
        if val != row['pmonth']:
            updatesql.append(' pmonth = %s' % (val,)) 
            logmsg.append( 'PMonth O:%s N:%s' % (row['pmonth'], val))
    if prod.data.get('precip_day'):
        val = prod.data['precip_day']
        if val != row['pday']:
            updatesql.append(' pday = %s' % (val,)) 
            logmsg.append( 'PDay O:%s N:%s' % (row['pday'], val))
    if len(updatesql) > 0:
        txn.execute("""UPDATE """+table+""" d SET """+ ','.join( updatesql ) +"""
         FROM stations t WHERE t.iemid = d.iemid and d.day = %s and t.id = %s
         and t.network ~* 'ASOS' """, (prod.cli_valid, station))
        print "%s rows for %s (%s) %s" % (txn.rowcount, station,
                                    prod.cli_valid.strftime("%y%m%d"),  
                                    ','.join( logmsg ) )

ldmbridge.LDMProductFactory( MyProductIngestor() )

reactor.run()