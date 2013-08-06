# Copyright (c) 2005 Iowa State University
# http://mesonet.agron.iastate.edu/ -- mailto:akrherz@iastate.edu
#
# This program is free software; you can redistribute it and/or modify it under
# the terms of the GNU General Public License as published by the Free Software
# Foundation; either version 2 of the License, or (at your option) any later
# version.
#
# This program is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
# FOR A PARTICULAR PURPOSE. See the GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License along with
# this program; if not, write to the Free Software Foundation, Inc.,
# 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
""" LSR product ingestor """

#stdlib
import pickle
import os
import datetime

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
    wfo = prod.source[1:]
    
    xtra = {
            'product_id': prod.get_product_id(),
            'channels': wfo,
            }

    if len(prod.lsrs) == 0:
        raise Exception("No LSRs parsed!", text)

    for lsr in prod.lsrs:
        if not reference.lsr_events.has_key(lsr.typetext):
            errmsg = "Unknown LSR typecode '%s'" % (lsr.typetext,)
            common.email_error(errmsg, text)
        uniquekey = hash(lsr.text)
        if LSRDB.has_key( uniquekey ):
            prod.duplicates += 1
            continue
        LSRDB[ uniquekey ] = datetime.datetime.utcnow().replace(
                                                tzinfo=pytz.timezone("UTC"))
        uri =  "%s#%s/%s/%s" % (common.config.get('urls', 'lsr'), wfo, 
                lsr.utcvalid.strftime("%Y%m%d%H%M"),
                lsr.utcvalid.strftime("%Y%m%d%H%M") )
        jabber_text, jabber_html = lsr.get_jabbers(uri)
        JABBER.sendMessage(jabber_text, jabber_html, xtra)
        common.tweet([wfo,], lsr.tweet(), uri, {'lat': str(lsr.get_lat()), 
                                            'long': str(lsr.get_lon())})

        lsr.sql(txn)

    if prod.is_summary():
        baseuri = common.config.get('urls', 'lsr')
        jabber_text, jabber_html = prod.get_jabbers(baseuri) 
        JABBER.sendMessage(jabber_text, jabber_html, xtra)
        common.tweet([wfo,], "Summary Local Storm Report", 
                     prod.get_url(baseuri))

reactor.callLater(0, loaddb)
JABBER = common.make_jabber_client("lsr_parser")
LDM = ldmbridge.LDMProductFactory( MyProductIngestor() )
reactor.callLater( 20, cleandb)
reactor.run()

