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

__revision__ = '$Id: afos_dump.py 4513 2009-01-06 16:57:49Z akrherz $'

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

log.startLogging(open('logs/afos_dump.log','a'))
log.FileLogObserver.timeFormat = "%Y/%m/%d %H:%M:%S %Z"

DBPOOL = adbapi.ConnectionPool("psycopg2", database="afos", 
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
    msg['subject'] = 'afos_dump.py Traceback'
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
    nws = TextProduct.TextProduct( buf, bypass=True)
    nws.findAFOS()
    data = re.sub("'", "\\'",nws.raw)
    data = re.sub("\x01", "", data)
    data = re.sub("\x00", "", data)

    DBPOOL.runOperation("""INSERT into products(pil,data)
      VALUES('%s','%s')""" % (nws.afos.strip(), 
      data) ).addErrback( email_error, buf)


ldm = ldmbridge.LDMProductFactory( MyProductIngestor() )
reactor.run()
