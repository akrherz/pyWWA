"""Save our local NEXRAD level 3 RADARs to CyBox"""
import datetime
import os
import subprocess
from pyiem.util import send2box

NEXRADS = "DMX DVN OAX ARX FSD MPX EAX ABR UDX".split()


def run(date):
    for nexrad in NEXRADS:
        os.chdir("/mnt/nexrad3/nexrad/NIDS/%s" % (nexrad, ))
        cmd = ("tar -czf /mesonet/tmp/%sradar.tgz ???/???_%s_*"
               ) % (nexrad, date.strftime("%Y%m%d"))
        subprocess.call(cmd, shell=True)

    filenames = ['/mesonet/tmp/%sradar.tgz' % (x, ) for x in NEXRADS]
    remotenames = ['%s_%s.tgz' % (x, date.strftime("%Y%m%d")) for x in NEXRADS]
    send2box(filenames, "/IowaNexrad3/%s" % (date.strftime("%Y/%m"),),
             remotenames=remotenames)
    for filename in filenames:
        if os.path.isfile(filename):
            os.unlink(filename)


def main():
    yesterday = datetime.date.today() - datetime.timedelta(days=1)
    run(yesterday)


if __name__ == '__main__':
    main()
