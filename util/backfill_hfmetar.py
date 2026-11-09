"""Sometimes LDM runs dry, but the data is on the website.

crontab job run at :55 after for the previous hour.
"""

import os
import subprocess
from datetime import timedelta

import requests
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
    try:
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
    except requests.RequestException as exp:
        LOG.warning("failed to fetch %s with %s", url, exp)
        return
    with open(f"{now:%Y%m%d_%H}00_partial.gz", "wb") as fh:
        fh.write(resp.content)
    subprocess.call(["gunzip", f"{now:%Y%m%d_%H}00_partial.gz"])
    subprocess.call(["mv", f"{now:%Y%m%d_%H}00_partial", testfn])
    LOG.warning("backfilled %s", testfn)


if __name__ == "__main__":
    main()
