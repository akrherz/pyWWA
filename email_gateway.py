# Route emails into iemchat, hmmmmm

import secret
import email, tempfile
import mimetypes, mx.DateTime
import sys, os, datetime, re, StringIO, traceback, smtplib
from email.MIMEText import MIMEText

os.chdir("/home/ldm/pyWWA/")

from twisted.words.protocols.jabber import client, jid
from twisted.words.xish import domish
from twisted.internet import reactor

qu = 0
class JabberClient:
    xmlstream = None

    def __init__(self, myJid):
        self.myJid = myJid


    def authd(self,xmlstream):
        print "authenticated"
        self.xmlstream = xmlstream
        presence = domish.Element(('jabber:client','presence'))
        xmlstream.send(presence)

        xmlstream.addObserver('/message',  self.debug)
        xmlstream.addObserver('/presence', self.debug)
        xmlstream.addObserver('/iq',       self.debug)

    def debug(self, elem):
        print elem.toXml().encode('utf-8')
        print "="*20

    def sendMessage(self, body, htmlbody):
        global qu
        while (self.xmlstream is None):
            print "xmlstream is None, so lets try again!"
            reactor.callLater(3, self.sendMessage, body, htmlbody)
            return

        message = domish.Element(('jabber:client','message'))
        message['to'] = "iembot@%s" % (secret.chatserver,)
        message['type'] = 'chat'
        message.addElement('body',None,body)

        html = domish.Element(('http://jabber.org/protocol/xhtml-im','html'))
        body = domish.Element(('http://www.w3.org/1999/xhtml','body'))
        body.addRawXml( htmlbody )
        html.addChild(body)
        message.addChild(html)
        self.xmlstream.send(message)
        qu -= 1

def process():
  try:
    process_message( sys.stdin )
  except:
    io = StringIO.StringIO()
    traceback.print_exc(file=io)
    msg = MIMEText("%s\n" % (io.getvalue(),) )
    msg['subject'] = 'iembot gateway traceback'
    msg['From'] = secret.parser_user
    msg['To'] = "akrherz@iastate.edu"

    s = smtplib.SMTP()
    s.connect()
    s.sendmail(msg["From"], msg["To"], msg.as_string())
    s.close()

def process_message( data ):
  global qu
  # Read message, data supports .read()
  msg = email.message_from_file( data )


  # messages are keyed based on
  # akrherz+service@host
  service = msg['to'].split("@")[0].split("+")[1]
  pil = secret.srvlkp[service]['pil']
  if (pil == "TBW-0"):
    body = "TBW: Updated GraphiCast http://www.srh.noaa.gov/tbw/html/tbw/Graphicast.php"
    htmlbody = "TBW has updated the <a href=\"http://www.srh.noaa.gov/tbw/html/tbw/Graphicast.php\">GraphiCast</a>"
    jabber.sendMessage(body, htmlbody)
    return
  if (pil == "TBW-1"):
    body = "TBW: Updated Graphical Daily Weather Hazards http://www.srh.noaa.gov/tbw/html/tbw/ghwo.htm"
    htmlbody = "TBW has updated the <a href=\"http://www.srh.noaa.gov/tbw/html/tbw/ghwo.htm\">Graphical Daily Weather Hazards</a>"
    jabber.sendMessage(body, htmlbody)
    return
  if (pil == "TBW-2"):
    body = "TBW: Updated Graphical Tropical Weather Hazards http://www.srh.noaa.gov/tbw/html/tbw/ghls.htm"
    htmlbody = "TBW has updated the <a href=\"http://www.srh.noaa.gov/tbw/html/tbw/ghls.htm\">Graphical Tropical Weather Hazards</a>"
    jabber.sendMessage(body, htmlbody)
    return

  ts = mx.DateTime.gmt().strftime("%d%H%M")
  edesc = "109 \nZZZZ63 K%s %s\n%s\n\n" % (pil[3:], ts, pil)
  for part in msg.walk():
    # multipart/* are just containers
    if part.get_content_maintype() == 'multipart':
      continue
    filename = part.get_filename()
    if not filename:
      stuff = part.get_payload(decode=True)
      if stuff:
        edesc += "\n".join( stuff.split("\n")[7:] )

  fname = tempfile.mktemp()
  fd = open(fname, 'w')
  fd.write( edesc +"\003")
  fd.close()

  cmd = "/home/ldm/bin/pqinsert -f WMO -p '/p%s' %s" % (pil, fname)
  os.system(cmd)

def killer():
    global qu
    print "queue", qu
    if (qu == 0):
        reactor.stop()
    reactor.callLater(10, killer)

myJid = jid.JID('%s@%s/ingestor_%s' % \
      (secret.iembot_ingest_user, secret.chatserver, \
       mx.DateTime.gmt().strftime("%Y%m%d%H%M%S") ) )
factory = client.basicClientFactory(myJid, secret.iembot_ingest_password)

jabber = JabberClient(myJid)

factory.addBootstrap('//event/stream/authd',jabber.authd)
factory.addBootstrap("//event/client/basicauth/invaliduser", jabber.debug)
factory.addBootstrap("//event/client/basicauth/authfailed", jabber.debug)
factory.addBootstrap("//event/stream/error", jabber.debug)

reactor.connectTCP(secret.connect_chatserver,5222,factory)
reactor.callLater(0, process)
reactor.callLater(10, killer)
reactor.run()




