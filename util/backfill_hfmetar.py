"""Sometimes LDM runs dry, but the data is on the website.

crontab job run at :55 after for the previous hour.
"""

import os
import subprocess
from datetime import timedelta

import httpx
from pyiem.util import logger, utc

LOG = logger()


def main():
    """Go Main Go."""
    os.chdir("/mesonet/data/madis/hfmetar")
    now = utc() - timedelta(hours=1)
    testfn = f"{now:%Y%m%d_%H}00_0.nc"
    if os.path.isfile(testfn):
        return
    url = (
        "https://madis-data.cprk.ncep.noaa.gov/madisPublic1/data/LDAD/hfmetar/"
        f"netCDF/{now:%Y%m%d_%H}00.gz"
    )
    LOG.info("fetching %s", url)
    resp = httpx.get(url, timeout=30)
    if resp.status_code != 200:
        LOG.warning("failed to fetch %s", url)
        return
    with open(f"{now:%Y%m%d_%H}00.gz", "wb") as fh:
        fh.write(resp.content)
    subprocess.call(["gunzip", f"{now:%Y%m%d_%H}00.gz"])
    subprocess.call(["mv", f"{now:%Y%m%d_%H}00", testfn])
    LOG.warning("backfilled %s", testfn)


if __name__ == "__main__":
    main()
