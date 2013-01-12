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
""" Twisted Way to dump data to the database """

# Twisted Python imports
from twisted.python import log, logfile
log.FileLogObserver.timeFormat = "%Y/%m/%d %H:%M:%S %Z"
log.startLogging(logfile.DailyLogFile('afos_dump.log','logs/'))

from twisted.internet import reactor
from twisted.enterprise import adbapi

import os

import ConfigParser
config = ConfigParser.ConfigParser()
config.read(os.path.join(os.path.dirname(__file__), 'cfg.ini'))

from pyldm import ldmbridge
from pyiem.nws import product
import common
import datetime
import pytz

DBPOOL = adbapi.ConnectionPool("twistedpg", database="afos", cp_reconnect=True,
                                host=config.get('database','host'), 
                                user=config.get('database','user'),
                                password=config.get('database','password') )

# LDM Ingestor
class MyProductIngestor(ldmbridge.LDMProductReceiver):
    """ I receive products from ldmbridge and process them 1 by 1 :) """

    def connectionLost(self, reason):
        log.msg('connectionLost')
        log.err( reason )
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
    """ Actually do something with the buffer, please """
    if buf.strip() == "":
        return
    utcnow = datetime.datetime.utcnow()
    utcnow = utcnow.replace(tzinfo=pytz.timezone("UTC"))
    
    nws = product.TextProduct( buf)

    if (utcnow - nws.valid).days > 180 or (utcnow - nws.valid).days < -180:
        common.email_error("Very Latent Product! %s" % (nws.valid,), nws.text)
        return
    
    if nws.valid.month > 6:
        table = "products_%s_0712" % (nws.valid.year,)
    else:
        table = "products_%s_0106" % (nws.valid.year,)
        
    
    df = DBPOOL.runOperation("""INSERT into """+table+"""(pil, data, entered,
        source, wmo) VALUES(%s,%s,%s,%s,%s)""",  (nws.afos.strip(), nws.text, 
                             nws.valid,
                             nws.source, nws.wmo) 
     )
    df.addErrback( common.email_error, buf)
    df.addErrback( log.err )

ldm = ldmbridge.LDMProductFactory( MyProductIngestor() )
reactor.run()
