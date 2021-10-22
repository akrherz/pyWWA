"""Save our local NEXRAD level 3 RADARs to staging."""
import datetime
import os
import subprocess

from pyiem.util import logger

LOG = logger()
NEXRADS = "DMX DVN OAX ARX FSD MPX EAX ABR UDX".split()


def run(date):
    """Process this date please"""
    yyyymmdd = date.strftime("%Y%m%d")
    for nexrad in NEXRADS:
        mydir = f"/mnt/nexrad3/nexrad/NIDS/{nexrad}"
        if not os.path.isdir(mydir):
            LOG.info("creating %s", mydir)
            os.makedirs(mydir)
        os.chdir(mydir)
        cmd = ("tar -czf /mesonet/tmp/%s_%s.tgz ???/???_%s_*") % (
            nexrad,
            yyyymmdd,
            yyyymmdd,
        )
        LOG.debug(cmd)
        proc = subprocess.Popen(
            cmd,
            shell=True,
            stderr=subprocess.PIPE,
            stdout=subprocess.PIPE,
        )
        (stdout, stderr) = proc.communicate()
        if stdout != b"" or stderr != b"":
            LOG.info(
                "%s resulted in\nstdout: %s\nstderr: %s",
                cmd,
                stdout.decode("ascii", "ignore"),
                stderr.decode("ascii", "ignore"),
            )

    rpath = f"/stage/IowaNexrad3/{date:%Y/%m}"
    cmd = (
        'rsync --remove-source-files -a --rsync-path "mkdir -p %s && rsync" '
        "/mesonet/tmp/???_%s.tgz meteor_ldm@metl60.agron.iastate.edu:%s"
    ) % (rpath, yyyymmdd, rpath)
    LOG.debug(cmd)
    subprocess.call(cmd, shell=True)


def main():
    """Go Main Go"""
    yesterday = datetime.date.today() - datetime.timedelta(days=1)
    run(yesterday)


if __name__ == "__main__":
    main()
