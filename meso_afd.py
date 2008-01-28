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
""" Hacky meso AFD processor """


import sys, re
import traceback
import StringIO
import secret
from pyIEM import nws_text
from pyxmpp.jid import JID
from pyxmpp.jabber.simple import send_message

errors = StringIO.StringIO()

from support import TextProduct

raw = sys.stdin.read()

import pg
POSTGIS = pg.connect(secret.dbname, secret.dbhost, user=secret.dbuser)


def calldb(sql):
    try:
        postgisdb.query(sql)
    except:
        errors.write("\n-----------\nSQL: %s\n" % (sql,) )
        traceback.print_exc(file=errors)
        errors.write("\n-----------\n")

def querydb(sql):
    try:
        return postgisdb.query(sql).dictresult()
    except:
        errors.write("\n-----------\nSQL: %s\n" % (sql,) )
        traceback.print_exc(file=errors)
        errors.write("\n-----------\n")

    return []

def sendJabberMessage(jabberTxt):
    jid=JID("iembot_ingestor@%s/Ingestor" % (secret.chatserver,) )
    recpt=JID("iembot@%s/Echobot" % (secret.chatserver,) )
    send_message(jid, secret.iembot_ingest_password, recpt, jabberTxt, 'Ba')

def process(raw):
    afos = sys.argv[1]
    pil = afos[:3]
    wfo = afos[3:]
    sqlraw = raw.replace("'", "\\'")

    tokens = re.findall("\.UPDATE\.\.\.MESOSCALE UPDATE", raw)
    if (len(tokens) == 0):
        return

    prod = TextProduct.TextProduct(raw)
    product_id = prod.get_product_id()
    sql = "INSERT into text_products(product, product_id) \
      values ('%s','%s')" % (sqlraw, product_id)
    POSTGIS.query(sql)

    mess = "%s: %s issues Mesoscale %s http://mesonet.agron.iastate.edu/p.php?pid=%s" % \
        (wfo, wfo, pil, product_id)
    sendJabberMessage(mess)
                
    errors.seek(0)
    print errors.read()
if __name__ == "__main__":
    process(raw)

