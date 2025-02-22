"""Archive MADIS files."""

import glob
import os
import subprocess
from datetime import date, timedelta

from pyiem.util import logger

LOG = logger()
HOST = "akrherz-desktop.agron.iastate.edu"


def process(mydir, valid):
    """Do the necessary work."""
    fulldir = f"/mesonet/data/madis/{mydir}"
    if not os.path.isdir(fulldir):
        LOG.info("Directory %s does not exist", fulldir)
        return
    os.chdir(fulldir)
    # Collapse files down.
    for hr in range(0, 24):
        i = 300
        found = False
        while i >= 0:
            fn = f"{valid:%Y%m%d}_{hr:02.0f}00_{i}.nc"
            if os.path.isfile(fn):
                if not found:
                    found = True
                    cmd = ["cp", "-f", fn, f"{valid:%Y%m%d}_{hr:02.0f}00.nc"]
                    subprocess.call(cmd)
                os.unlink(fn)
            i -= 1
    # gzip
    files = glob.glob(f"{valid:%Y%m%d}*nc")
    if not files:
        LOG.info("No files found for %s", mydir)
        return
    subprocess.call(["gzip", *files])
    files = glob.glob(f"{valid:%Y%m%d}*nc.gz")
    if not files:
        LOG.info("No gzip files found for %s", mydir)
        return
    # rsync
    cmd = [
        "rsync",
        "-a",
        "--remove-source-files",
        "--rsync-path",
        f"mkdir -p /stage/iemoffline/madis/{mydir}/{valid:%Y} && rsync",
        *files,
        f"{HOST}:/stage/iemoffline/madis/{mydir}/{valid:%Y}/",
    ]
    subprocess.call(cmd)


def main():
    """Go Main Go."""
    valid = date.today() - timedelta(days=1)
    for mydir in ["hfmetar", "mesonet1", "metar", "rwis1"]:
        process(mydir, valid)


if __name__ == "__main__":
    main()
