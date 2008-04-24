# Route emails into iemchat, hmmmmm

import secret
import email, tempfile
import mimetypes, mx.DateTime
import sys, os, datetime, re, StringIO, traceback, smtplib
from email.MIMEText import MIMEText


os.chdir("/home/ldm/pyWWA/")

def process_message( data ):
  # Read message, data supports .read()
  msg = email.message_from_file( data )


  # messages are keyed based on
  # akrherz+service@host
  service = msg['to'].split("@")[0].split("+")[1]
  pil = secret.srvlkp[service]['pil']

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

if (__name__ == "__main__"):
  try:
    process_message( sys.stdin )
  except:
    io = StringIO.StringIO()
    traceback.print_exc(file=io)
    msg = MIMEText("%s\n" % (io.getvalue(),) )
    msg['subject'] = 'iembot gateway traceback'
    msg['From'] = "ldm@mesonet.agron.iastate.edu"
    msg['To'] = "akrherz@iastate.edu"

    s = smtplib.SMTP()
    s.connect()
    s.sendmail(msg["From"], msg["To"], msg.as_string())
    s.close()

