"""Feed archived NCR data to nexrad3_attr.py , should be run like

python util/feed_archived_ncr.py NEXRAD YYYY MM
  | YYYY=2009 MM=01 python nexrad3_attr.py
"""

import datetime
import glob
import os
import subprocess
import sys
import time


def main(argv):
    """Go Main Go."""
    if not os.path.isdir("/tmp/l3tmp"):
        os.makedirs("/tmp/l3tmp")

    nexrad = sys.argv[1]

    sts = datetime.datetime(int(argv[2]), int(argv[3]), 1)
    ets = sts + datetime.timedelta(days=32)
    ets = ets.replace(day=1)
    interval = datetime.timedelta(days=1)

    now = sts
    while now < ets:
        afn = now.strftime(
            f"/mesonet/ARCHIVE/nexrad/%Y_%m/{nexrad}_%Y%m%d.tgz"
        )
        if not os.path.isfile(afn):
            sys.stderr.write(f"Missing {afn}\n")
            now += interval
            continue

        subprocess.call(f"tar -x -z -C /tmp/l3tmp -f {afn} NCR", shell=True)
        if not os.path.isdir("/tmp/l3tmp/NCR"):
            sys.stderr.write(f"Missing NCR data for {afn}\n")
            now += interval
            continue

        files = glob.glob("/tmp/l3tmp/NCR/NCR_*")
        for fn in files:
            # Need fake seq id
            with open(fn, "rb") as fh:
                data = fh.read()
            if len(data) < 20:
                sys.stderr.write(f"short read on {fn}\n")
                continue
            if data[0] == "S":
                sys.stdout.write("\001\r\r\n000\r\r\n")
            sys.stdout.write(data)
            if data[-4:] != "\r\r\n\003":
                sys.stdout.write("\r\r\n\003")
            time.sleep(0.25)

        if os.path.isdir("/tmp/l3tmp/NCR"):
            subprocess.call("rm -rf /tmp/l3tmp/NCR", shell=True)

        now += interval


if __name__ == "__main__":
    main(sys.argv)
