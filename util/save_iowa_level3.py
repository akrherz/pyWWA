"""Save our local NEXRAD level 3 RADARs to staging."""

import os
import subprocess
from datetime import date, datetime, timedelta

from pyiem.util import logger

LOG = logger()
NEXRADS = "DMX DVN OAX ARX FSD MPX EAX ABR UDX".split()


def run(dt: datetime):
    """Process this date please"""
    yyyymmdd = dt.strftime("%Y%m%d")
    for nexrad in NEXRADS:
        mydir = f"/data/gempak/nexrad/NIDS/{nexrad}"
        if not os.path.isdir(mydir):
            LOG.info("creating %s", mydir)
            os.makedirs(mydir)
        os.chdir(mydir)
        cmd = (
            f"tar -czf /mesonet/tmp/{nexrad}_{yyyymmdd}.tgz "
            f"???/???_{yyyymmdd}_*"
        )
        LOG.debug(cmd)
        with subprocess.Popen(
            cmd,
            shell=True,
            stderr=subprocess.PIPE,
            stdout=subprocess.PIPE,
        ) as proc:
            (stdout, stderr) = proc.communicate()
        if stdout != b"" or stderr != b"":
            LOG.info(
                "%s resulted in\nstdout: %s\nstderr: %s",
                cmd,
                stdout.decode("ascii", "ignore"),
                stderr.decode("ascii", "ignore"),
            )

    rpath = f"/stage/IowaNexrad3/{dt:%Y/%m}"
    cmd = (
        "rsync --remove-source-files -a --rsync-path "
        f'"mkdir -p {rpath} && rsync" '
        f"/mesonet/tmp/???_{yyyymmdd}.tgz "
        f"meteor_ldm@akrherz-desktop.agron.iastate.edu:{rpath}"
    )
    LOG.debug(cmd)
    subprocess.call(cmd, shell=True)


def main():
    """Go Main Go"""
    yesterday = date.today() - timedelta(days=1)
    run(yesterday)


if __name__ == "__main__":
    main()
