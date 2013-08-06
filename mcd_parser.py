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
""" MCD product ingestor 
"""

from twisted.python import log
from twisted.python import logfile
log.FileLogObserver.timeFormat = "%Y/%m/%d %H:%M:%S %Z"
log.startLogging(logfile.DailyLogFile('mcd_parser.log', 'logs'))

from pyldm import ldmbridge
from pyiem.nws.products.mcd import parser as mcdparser
import common

from twisted.internet import reactor
from twisted.enterprise import adbapi
from shapely.geometry import MultiPolygon

DBPOOL = adbapi.ConnectionPool("twistedpg", database="postgis", cp_max=1,
                           host=common.config.get('database','host'), 
                           user=common.config.get('database','user'),
                           password=common.config.get('database','password'), 
                           cp_reconnect=True)
# LDM Ingestor
class MyProductIngestor(ldmbridge.LDMProductReceiver):
    """ I receive products from ldmbridge and process them 1 by 1 :) """

    def connectionLost(self, reason):
        ''' connection was lost for some reason '''
        log.msg('connectionLost')
        log.err( reason )
        reactor.callLater(5, reactor.callWhenRunning, reactor.stop)

    def process_data(self, raw):
        ''' process the data, please '''
        defer = DBPOOL.runInteraction(real_process, raw)
        defer.addErrback( common.email_error, raw)

def real_process(txn, raw):
    ''' Actually do stuff! '''
    mcd = mcdparser( raw )
    product_id = mcd.get_product_id()
    xtra = {
            'product_id': product_id,
            'channels': ','.join(mcd.attn_wfo) + ',SPC'
            }

    # Yuck, the database has a multi-polygon requirement
    multi = MultiPolygon([mcd.geometry])
    wkt = "SRID=4326;%s" % (multi.wkt,)
    sql = """INSERT into text_products(product, product_id, geom) 
      values (%s,%s, %s)""" 
    sqlargs = (raw, product_id, wkt)
    txn.execute(sql, sqlargs)

    uri = '%s?pid=%s' % (common.config.get('urls', 'product'), product_id)
    body, htmlbody = mcd.get_jabbers(uri)
    JABBER.sendMessage(body, htmlbody, xtra)

    # Twitter this
    twt = mcd.tweet()
    url = mcd.get_spc_url()
    common.tweet(xtra['channels'].split(','), twt, url) 

JABBER = common.make_jabber_client("mcd_parser")

ldmbridge.LDMProductFactory( MyProductIngestor() )

reactor.run()
