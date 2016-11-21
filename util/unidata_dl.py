import glob
import os
import datetime

sts = datetime.datetime(2016, 11, 21, 0, 0)
ets = datetime.datetime(2016, 11, 21, 3, 0)
interval = datetime.timedelta(minutes=1)

os.chdir('data/nexrad/NIDS')
for nexrad in glob.glob('???'):
    os.chdir(nexrad)
    for nids in ['N0Q', 'NET', 'N0R', 'EET']:
        if not os.path.isdir(nids):
            continue
        os.chdir(nids)
        now = sts
        while now < ets:
            fp = "%s_%s" % (nids, now.strftime("%Y%m%d_%H%M"))
            if not os.path.isfile(fp):
                url = now.strftime(("http://motherlode.ucar.edu/native/radar/"
                                    "level3/" + nids + "/" + nexrad +
                                    "/%Y%m%d/Level3_" + nexrad + "_" + nids +
                                    "_%Y%m%d_%H%M.nids"))
                cmd = "wget -q -O %s %s" % (fp, url)
                os.system(cmd)
            now += interval
        os.chdir('..')
    os.chdir('..')
