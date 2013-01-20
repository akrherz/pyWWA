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
""" SPS product ingestor 
"""

from twisted.python import log
from twisted.python import logfile
import os
log.FileLogObserver.timeFormat = "%Y/%m/%d %H:%M:%S %Z"
log.startLogging( logfile.DailyLogFile('sps_parser.log','logs/'))

import datetime
import common
from pyiem.nws import product
from pyiem import reference
from pyldm import ldmbridge

from twisted.internet import reactor
from twisted.enterprise import adbapi

import ConfigParser
config = ConfigParser.ConfigParser()
config.read(os.path.join(os.path.dirname(__file__), 'cfg.ini'))

POSTGIS = adbapi.ConnectionPool("twistedpg", database="postgis", 
                                cp_reconnect=True, cp_max=1,
                                host=config.get('database','host'), 
                                user=config.get('database','user'),
                                password=config.get('database','password') )


ugc_dict = {}
def load_ugc(txn):
    """ load ugc dict """
    sql = "SELECT name, ugc from nws_ugc WHERE name IS NOT Null"
    txn.execute( sql )
    for row in txn:
        name = (row["name"]).replace("\x92"," ")
        ugc_dict[ row['ugc'] ] = name

    log.msg("ugc_dict is loaded...")


def countyText(ugcs):
    countyState = {}
    c = ""
    for ugc in ugcs:
        stateAB = ugc.state
        if not countyState.has_key(stateAB):
            countyState[stateAB] = []
        if not ugc_dict.has_key(str(ugc)):
            name = "((%s))" % (str(ugc),)
        else:
            name = ugc_dict[str(ugc)]
        countyState[stateAB].append(name)

    for st in countyState.keys():
        countyState[stateAB].sort()
        c +=" %s [%s] and" %(", ".join(countyState[st]), st)
    return c[:-4]




# LDM Ingestor
class myProductIngestor(ldmbridge.LDMProductReceiver):

    def process_data(self, buf):
        try:
            real_process(buf)
        except Exception, myexp:
            common.email_error(myexp, buf)

    def connectionLost(self, reason):
        log.msg('connectionLost') 
        log.err(reason)
        reactor.callLater(5, self.shutdown)

    def shutdown(self):
        reactor.callWhenRunning(reactor.stop)


def real_process(raw):
    sqlraw = raw.replace("\015\015\012", "\n")
    prod = product.TextProduct(raw)

    product_id = prod.get_product_id()
    if len(prod.segments) == 0:
        # Send the office an alert that they probably don't have a trailing
        # $$
        print "Missing $$ detected", product_id
        mess = "%s: iembot failed to process product: %s\nError: %s" % \
               (prod.get_iembot_source(), product_id, "Missing $$")
        htmlmess = "<span style='color: #FF0000; font-weight: bold;'>\
iembot processing error:</span><br />Product: %s<br />Error: %s" % \
            (product_id, "Missing $$")
        jabber.sendMessage(mess, htmlmess)
        return


    if prod.segments[0].giswkt:
        sql = """INSERT into text_products(product, product_id, geom) values (%s,%s, %s)"""
        myargs = (sqlraw, product_id, prod.segments[0].giswkt )
        
    else:
        sql = "INSERT into text_products(product, product_id) values (%s,%s)" 
        myargs = (sqlraw, product_id)
    deffer = POSTGIS.runOperation(sql, myargs)
    deffer.addErrback( common.email_error, sqlraw )
    deffer.addErrback( log.err )

    for seg in prod.segments:
        headline = "[NO HEADLINE FOUND IN SPS]"
        if len(seg.headlines) > 0:
            headline = (seg.headlines[0]).replace("\n", " ")
        elif raw.find("SPECIAL WEATHER STATEMENT") > 0:
            headline = "Special Weather Statement"
        counties = countyText(seg.ugcs)
        if (counties.strip() == ""):
            counties = "entire area"
        expire = ""
        if seg.ugcexpire is not None:
            expire = "till %s %s" % (
                (seg.ugcexpire - datetime.timedelta(
                hours=reference.offsets.get(prod.z,0) )).strftime("%-I:%M %p"),
                                      prod.z)


        mess = "%s: %s issues %s for %s %s %s?pid=%s" % (prod.source[1:], 
           prod.source[1:], headline, counties, expire, 
           config.get('urls', 'product'), product_id)
        htmlmess = "%s issues <a href='%s?pid=%s'>%s</a> for %s %s" % ( 
                                prod.source[1:], config.get('urls', 'product'), 
                                product_id, headline, counties, expire)
        log.msg(mess)

        jabber.sendMessage(mess, htmlmess)

        twt = "%s for %s %s" % (headline, counties, expire)
        url = "%s?pid=%s" % (config.get('urls', 'product'), product_id)
        common.tweet([prod.source[1:],], twt, url)

jabber = common.make_jabber_client('sps_parser')

def ready(bogus):
    ldmbridge.LDMProductFactory( myProductIngestor() )

def killer(err):
    log.err( err )
    reactor.stop()

df = POSTGIS.runInteraction(load_ugc)
df.addCallback(ready)
df.addErrback( killer )

reactor.run()
