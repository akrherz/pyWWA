""" LSR product ingestor """

#stdlib
import pickle
import os
# http://bugs.python.org/issue7980
import datetime
datetime.datetime.strptime('2013', '%Y')

# Get the logger going, asap
from twisted.python import log
from twisted.python import logfile
log.FileLogObserver.timeFormat = "%Y/%m/%d %H:%M:%S %Z"
log.startLogging(logfile.DailyLogFile('lsr_parser.log', 'logs'))

# Third party python stuff
import pytz
from twisted.enterprise import adbapi
from twisted.internet import reactor
from pyiem import reference
from pyiem.nws.products.lsr import parser as lsrparser
from pyldm import ldmbridge

# IEM python Stuff
import common

DBPOOL = adbapi.ConnectionPool("twistedpg", database="postgis", 
                           cp_reconnect=True,
                           host=common.config.get('database','host'), 
                           user=common.config.get('database','user'),
                           password=common.config.get('database','password') )

# Cheap datastore for LSRs to avoid Dups!
LSRDB = {}
def loaddb():
    ''' load memory '''
    if os.path.isfile('lsrdb.p'):
        mydict = pickle.load( open('lsrdb.p') )
        for key in mydict:
            LSRDB[key] = mydict[key]

def cleandb():
    """ To keep LSRDB from growing too big, we clean it out 
        Lets hold 7 days of data!
    """
    utc = datetime.datetime.utcnow().replace(tzinfo=pytz.timezone("UTC"))
    thres = utc - datetime.timedelta(hours=24*7)
    init_size = len(LSRDB)
    for key in LSRDB.keys():
        if LSRDB[key] < thres:
            del LSRDB[key]

    fin_size = len(LSRDB)
    log.msg("cleandb() init_size: %s final_size: %s" % (init_size, fin_size))
    # Non blocking hackery
    reactor.callInThread(pickledb)

    # Call Again in 30 minutes
    reactor.callLater(60*30, cleandb) 

def pickledb():
    """ Dump our database to a flat file """
    pickle.dump(LSRDB, open('lsrdb.p','w'))

class MyProductIngestor(ldmbridge.LDMProductReceiver):
    """ I process products handed off to me from faithful LDM """

    def process_data(self, text):
        """ @buf: String that is a Text Product """
        defer = DBPOOL.runInteraction(real_processor, text)
        defer.addErrback( common.email_error, text )

    def connectionLost(self, reason):
        ''' the connection was lost! '''
        log.msg('connectionLost')
        log.err( reason )
        reactor.callLater(5, reactor.callWhenRunning, reactor.stop)

def real_processor(txn, text):
    """ Lets actually process! """
    prod = lsrparser( text )
    
    if len(prod.lsrs) == 0:
        raise Exception("No LSRs parsed!", text)

    for lsr in prod.lsrs:
        if not reference.lsr_events.has_key(lsr.typetext):
            errmsg = "Unknown LSR typecode '%s'" % (lsr.typetext,)
            common.email_error(errmsg, text)
        uniquekey = hash(lsr.text)
        if LSRDB.has_key( uniquekey ):
            prod.duplicates += 1
            lsr.duplicate = True
            continue
        LSRDB[ uniquekey ] = datetime.datetime.utcnow().replace(
                                                tzinfo=pytz.timezone("UTC"))
        lsr.sql(txn)

    j = prod.get_jabbers(common.config.get('urls', 'lsr')) 
    for (p, h, x) in j:
        JABBER.sendMessage(p, h, x)

reactor.callLater(0, loaddb)
JABBER = common.make_jabber_client("lsr_parser")
LDM = ldmbridge.LDMProductFactory( MyProductIngestor() )
reactor.callLater( 20, cleandb)
reactor.run()

