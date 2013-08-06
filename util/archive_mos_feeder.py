import subprocess
import time
import mx.DateTime

sts = mx.DateTime.DateTime(2000,7,1)
ets = mx.DateTime.DateTime(2000,9,1)
interval = mx.DateTime.RelativeDateTime(months=1)
now = sts
while now < ets:
    #uri = now.strftime("http://www.mdl.nws.noaa.gov/~mos/archives/avnmav/mav%Y%m.Z")
    #subprocess.call("wget %s" % (uri,), shell=True)

    #fn = now.strftime("mav%Y%m.Z")
    #subprocess.call("gunzip %s" % (fn,), shell=True)

    fn = now.strftime("mav%Y%m")
    counter = 0
    data = ""
    for line in open(fn):
        if line.strip() == "":
            counter += 1
        data += "%s\r\r\n" % (line.strip(),)
        if counter > 30:
            print data
            print '\003'
            data = ""
            counter = 0
    time.sleep(1800)
    now += interval


