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
""" ASOS Daily Summary Message Parser ingestor """


__revision__ = '$Id: dsm_parser.py 4513 2009-01-06 16:57:49Z akrherz $'

# Twisted Python imports
from twisted.internet import reactor
from twisted.python import log
from twisted.enterprise import adbapi
from twisted.mail import smtp

# Standard Python modules
import os, re, traceback, StringIO, smtplib
from email.MIMEText import MIMEText

# Python 3rd Party Add-Ons
import mx.DateTime, pg

# pyWWA stuff
from support import ldmbridge, TextProduct, reference
import secret
import common

log.startLogging(open('logs/dsm.log','a'))
log.FileLogObserver.timeFormat = "%Y/%m/%d %H:%M:%S %Z"

DBPOOL = adbapi.ConnectionPool("psycopg2", database="iem", 
                               host=secret.dbhost, password=secret.dbpass)
EMAILS = 10

def email_error(message, product_text):
    """
    Generic something to send email error messages 
    """
    global EMAILS
    log.msg( message )
    EMAILS -= 1
    if (EMAILS < 0):
        return

    msg = MIMEText("Exception:\n%s\n\nRaw Product:\n%s" \
                 % (message, product_text))
    msg['subject'] = 'dsm_parser.py Traceback'
    msg['From'] = secret.parser_user
    msg['To'] = secret.error_email
    smtp.sendmail("localhost", msg["From"], msg["To"], msg)

# LDM Ingestor
class MyProductIngestor(ldmbridge.LDMProductReceiver):
    """ I receive products from ldmbridge and process them 1 by 1 :) """

    def connectionLost(self, reason):
        print 'connectionLost', reason
        reactor.callLater(5, self.shutdown)

    def shutdown(self):
        reactor.callWhenRunning(reactor.stop)


    def process_data(self, buf):
        """ Process the product """
        try:
            real_parser(buf)
        except Exception, myexp:
            email_error(myexp, buf)

def real_parser(buf):
# KAMW DS 19/07 761539/ 540532// 76/ 54//0062028/00/00/00/00/00/00/00/
#00/00/00/00/00/00/00/00/00/00/00/00/00/00/00/00/00/00/25/12091717/
#14131452=
    # Split lines
    raw = buf.replace("\015\015\012", "\n")
    lines = raw.split("\n")
    if len(lines[3]) < 10:
        meat = ("".join(lines[4:])).split("=")
    else:
        meat = ("".join(lines[3:])).split("=")
    for data in meat:
        if data == "":
            continue
        process_dsm( data )

PARSER_RE = re.compile("""^(?P<id>[A-Z][A-Z0-9]{3})\s+
   DS\s+
   (COR\s)?
   ([0-9]{4}\s)?
   (?P<day>\d\d)/(?P<month>\d\d)\s?
   ((?P<highmiss>M)|((?P<high>(-?\d+))(?P<hightime>[0-9]{4})))/\s?
   ((?P<lowmiss>M)|((?P<low>(-?\d+))(?P<lowtime>[0-9]{4})))//\s?
   (?P<coophigh>(-?\d+|M))/\s?
   (?P<cooplow>(-?\d+|M))//
   (?P<minslp>M|[0-9]{3,4})(?P<slptime>[0-9]{4})?/
   (?P<precip>T|M|[0-9]{,4})/
 (?P<p01>T|M|\-|[0-9]{,4})/(?P<p02>T|M|\-|[0-9]{,4})/(?P<p03>T|M|\-|[0-9]{,4})/
 (?P<p04>T|M|\-|[0-9]{,4})/(?P<p05>T|M|\-|[0-9]{,4})/(?P<p06>T|M|\-|[0-9]{,4})/
 (?P<p07>T|M|\-|[0-9]{,4})/(?P<p08>T|M|\-|[0-9]{,4})/(?P<p09>T|M|\-|[0-9]{,4})/
 (?P<p10>T|M|\-|[0-9]{,4})/(?P<p11>T|M|\-|[0-9]{,4})/(?P<p12>T|M|\-|[0-9]{,4})/
 (?P<p13>T|M|\-|[0-9]{,4})/(?P<p14>T|M|\-|[0-9]{,4})/(?P<p15>T|M|\-|[0-9]{,4})/
 (?P<p16>T|M|\-|[0-9]{,4})/(?P<p17>T|M|\-|[0-9]{,4})/(?P<p18>T|M|\-|[0-9]{,4})/
 (?P<p19>T|M|\-|[0-9]{,4})/(?P<p20>T|M|\-|[0-9]{,4})/(?P<p21>T|M|\-|[0-9]{,4})/
 (?P<p22>T|M|\-|[0-9]{,4})/(?P<p23>T|M|\-|[0-9]{,4})/(?P<p24>T|M|\-|[0-9]{,4})/
   (?P<avg_sped>[0-9]{3})?/?
   (?P<drct_sped_max>[0-9]{2})?(?P<sped_max>[0-9]{2})?(?P<time_sped_max>[0-9]{4})?/?
   (?P<drct_gust_max>[0-9]{2})?(?P<sped_gust_max>[0-9]{2})?(?P<time_gust_max>[0-9]{4})?
""", re.VERBOSE)

def process_dsm(data):
    m = PARSER_RE.match( data )
    if m is None:
        print "FAIL!", data
        email_error("DSM RE Match Failure", data)
        return
    dict = m.groupdict()
    if dict['id'][0] != "K":
        return
    # Figure out the timestamp
    now = mx.DateTime.now()
    ts = now + mx.DateTime.RelativeDateTime(day=int(dict['day']),
               month=int(dict['month']))
    if ts.month == 12 and now.month == 1:
        ts -= mx.DateTime.RelativeDateTime(years=1)
    updater = []
    if dict['high'] != "M":
        updater.append("max_tmpf = %s" % (dict['high'],))
    if dict['low'] != "M":
        updater.append("min_tmpf = %s" % (dict['low'],))
    if dict['precip'] != "M" and dict['precip'] != "T":
        updater.append("pday = %s" % (float(dict['precip']) / 100.0,))
    if dict['precip'] == "T":
        updater.append("pday = 0.0001")

    sql = "UPDATE summary_%s SET %s WHERE station = '%s' and day = '%s'" % (
         ts.year, " , ".join(updater), dict['id'][1:], ts.strftime("%Y-%m-%d"))
    print "%s %s %s %s %s" % (dict['id'], ts.strftime("%Y-%m-%d"),
          dict['high'], dict['low'], dict['precip'] )
    DBPOOL.runOperation( sql ).addErrback( email_error, sql)

ldm = ldmbridge.LDMProductFactory( MyProductIngestor() )
reactor.run()
