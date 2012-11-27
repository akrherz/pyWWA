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

# pyWWA stuff
from support import ldmbridge, TextProduct
import common

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
    nws = TextProduct.TextProduct( buf, bypass=True)
    nws.findAFOS()
    nws.findIssueTime()
    nws.findWMO()
    
    DBPOOL.runOperation("""INSERT into products(pil, data, entered,
        source, wmo) VALUES(%s,%s,%s,%s,%s)""",  (nws.afos.strip(), nws.raw, 
                             nws.issueTime.strftime("%Y-%m-%d %H:%M+00"),
                             nws.source, nws.wmo) 
     ).addErrback( common.email_error, buf)


ldm = ldmbridge.LDMProductFactory( MyProductIngestor() )
reactor.run()
