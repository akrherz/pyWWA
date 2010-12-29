# Script to dump AFOS info, this could get nasty :)
#
# Daryl Herzmann 10 Jan 2002
# 18 Jan 2002:  Handle situation when database needs to be locked
#
#################################################################

import pg, sys, os, re, time

rejects = {'TSTNCF': ''}

def Main():
  if (len(sys.argv) < 2):
    return
  pil = sys.argv[1]
  if (rejects.has_key(pil)):
    myData = sys.stdin.read()
    return
  myData = sys.stdin.read()
  myData = re.sub("'", "\\'", myData)
  table = "products"
  #if (os.path.isfile('/tmp/AFOS.lock') ):
  #  table = "current2"

  i = 0
  while (i < 10):
    try:
      mydb = pg.connect("afos", "iemdb")
      break
    except:
      time.sleep(4)
      i += 1
  try:
    mydb.query("INSERT into "+table+"(pil,data) VALUES ('"+pil+"','"+myData+"')")
    mydb.close()
  except:
    o = open('/tmp/logggg', 'a')
    o.write("%s\n----------------%s\n" % (sys.excepthook(sys.exc_info()[0],sys.exc_info()[1],sys.exc_info()[2]), myData) )
    o.close()
    hello = "hi"

Main()
sys.exit(0)
