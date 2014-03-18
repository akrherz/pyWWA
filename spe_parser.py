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
""" SPENES product ingestor """

from twisted.python import log, logfile
import os
log.FileLogObserver.timeFormat = "%Y/%m/%d %H:%M:%S %Z"
log.startLogging( logfile.DailyLogFile('spe_parser.log', 'logs') )


import sys
import re
import common

from twisted.internet import reactor
from twisted.enterprise import adbapi

import ConfigParser
config = ConfigParser.ConfigParser()
config.read(os.path.join(os.path.dirname(__file__), 'cfg.ini'))

from pyiem.nws import product
POSTGIS = adbapi.ConnectionPool("twistedpg", database="postgis", cp_reconnect=True,
                                host=config.get('database','host'), 
                                user=config.get('database','user'),
                                password=config.get('database','password') )
raw = sys.stdin.read()

def process(raw):
    try:
        real_process(raw)
    except Exception, exp:
        common.email_error(exp, raw)


def real_process(raw):
    sqlraw = raw.replace("\015\015\012", "\n")
    prod = product.TextProduct(raw)

    product_id = prod.get_product_id()
    xtra ={
           'product_id': product_id,
           'channels': []
           }
    sql = """INSERT into text_products(product, product_id) values (%s,%s)"""
    myargs = (sqlraw, product_id)
    POSTGIS.runOperation(sql, myargs)
    
    tokens = re.findall("ATTN (WFOS|RFCS)(.*)", raw)
    for tpair in tokens:
        wfos = re.findall("([A-Z]+)\.\.\.", tpair[1])
        xtra['channels'] = []
        for wfo in wfos:
            xtra['channels'].append( wfo )
            twt = "#%s NESDIS issues Satellite Precipitation Estimates" % (wfo,)
            url = "%s?pid=%s" % (config.get('urls', 'product'), product_id)
            common.tweet(wfo, twt, url)

        body = "NESDIS issues Satellite Precipitation Estimates %s?pid=%s" % (
                config.get('urls', 'product'), product_id)
        htmlbody = "NESDIS issues <a href='%s?pid=%s'>Satellite Precipitation Estimates</a>" %(
                config.get('urls', 'product'), product_id)
        jabber.sendMessage(body, htmlbody, xtra)




def killer():
    reactor.stop()

jabber = common.make_jabber_client("spe_parser")
reactor.callLater(0, process, raw)
reactor.callLater(30, killer)
reactor.run()



