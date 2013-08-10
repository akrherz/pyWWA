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
""" 
=============================================================================
  * * * Only edit this upstream https://github.com/akrherz/pyWWA
=============================================================================

 Support SPC's MCD product
 Support WPC's FFG product
"""
from syslog import LOG_LOCAL2
from twisted.python import syslog
syslog.startLogging(prefix='pyWWA/mcd_parser', facility=LOG_LOCAL2)

from twisted.python import log
from twisted.enterprise import adbapi
from twisted.internet import reactor

from pyiem.nws.products.mcd import parser as mcdparser
from shapely.geometry import MultiPolygon
from pyldm import ldmbridge

import common

DBPOOL = adbapi.ConnectionPool("psycopg2", cp_max=2,
                       database=common.config.get('databaserw').get('postgis'),
                       host=common.config.get('databaserw').get('host'),
                       password=common.config.get('databaserw').get('password'),
                       user=common.config.get('databaserw').get('user'),
                       cp_reconnect=True)

common.write_pid("pyWWA_mcd_parser")


# LDM Ingestor
class MyProductIngestor(ldmbridge.LDMProductReceiver):
    """ I receive products from ldmbridge and process them 1 by 1 :) """

    def connectionLost(self, reason):
        ''' Called when the STDIN connection is lost '''
        log.msg('connectionLost')
        log.err( reason )
        reactor.callLater(15, reactor.callWhenRunning, reactor.stop)


    def process_data(self, raw):
        ''' Process a chunk of data '''
        df = DBPOOL.runInteraction(real_process, raw)
        df.addErrback(common.email_error, raw)

def real_process(txn, raw):
    """"
    Actually process a single MCD
    """
    prod = mcdparser(raw)
        
    product_id = prod.get_product_id()
    giswkt = "SRID=4326;%s" % (MultiPolygon([prod.geometry]).wkt,)
    sql = """INSERT into text_products(product, product_id, geom) 
      values (%s, %s, %s)"""
    args = (raw, product_id, giswkt)
    txn.execute(sql, args)

    channels = [prod.afos,]
    channels.extend( prod.attn_wfo )
    channels.extend( prod.attn_rfc )
    channels.extend( prod.find_cwsus(txn) )

    body, htmlbody = prod.get_jabbers(common.settings.get('pywwa_product_url', 
                                                      'pywwa_product_url'))
    JABBER.sendMessage(body, htmlbody, {
                        'channels': ",".join( channels ),
                        'product_id': product_id} )


ldmbridge.LDMProductFactory( MyProductIngestor() )
JABBER = common.make_jabber_client('mcdparse')

reactor.run()
