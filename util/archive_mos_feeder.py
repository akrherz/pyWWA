import subprocess
import mx.DateTime
import time

sts = mx.DateTime.DateTime(2002,2,1)
ets = mx.DateTime.DateTime(2007,5,1)
interval = mx.DateTime.RelativeDateTime(months=1)
now = sts
while now < ets:
    #for t in ['00','06','12','18']:
    for t in ['',]:
        #uri = now.strftime("http://www.mdl.nws.noaa.gov/~mos/archives/avnmav/mav%Y%m.t"+t+"z.Z")
        uri = now.strftime("http://www.mdl.nws.noaa.gov/~mos/archives/etamet/met%Y%m.Z")
        subprocess.call("wget %s" % (uri,), shell=True)

        #fn = now.strftime("mav%Y%m.t"+t+"z.Z")
        fn = now.strftime("met%Y%m.Z")
        subprocess.call("gunzip %s" % (fn,), shell=True)

        #fn = now.strftime("mav%Y%m.t"+t+"z")
        fn = now.strftime("met%Y%m")
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
                time.sleep(1.6)
    now += interval
