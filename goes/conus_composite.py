"""Generate some CONUS composites like we had back in the day.

1. gdalwarp: convert East and West into CONUS EPSG:4326 at desired res ~4km
   appears CONUS goes west goes to just -112 east?
2. gdal_merge.py: combine these two with the west file last (on top)
"""

import datetime
import json
import os
import subprocess
import sys
import tempfile
from zoneinfo import ZoneInfo

from pyiem.util import logger, utc

ISO = "%Y-%m-%dT%H:%M:%SZ"
LOG = logger()
PATH = "/mesonet/ldmdata/gis/images/GOES/conus"
LOOKUP = {2: "vis", 9: "wv", 13: "ir"}


def filtermsg(buf):
    """Less is more"""
    text = buf.decode("ascii")
    return "Point outside of projection" in text


def call(cmd):
    """Wrapper around subprocess."""
    LOG.debug(cmd)
    proc = subprocess.Popen(
        cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE
    )
    stdout = proc.stdout.read()
    stderr = proc.stderr.read()
    if stdout != b"":
        if not filtermsg(stdout):
            LOG.warning(cmd)
            LOG.warning(stdout)
    if stderr != b"":
        if not filtermsg(stderr):
            LOG.warning(cmd)
            LOG.warning(stderr)


def run(valid, channel, tmpname):
    """Do work."""
    tiles = []
    for bird in [16, 17, 18, 19]:
        fnbase = "%s/channel%02i/GOES-%s_C%02i" % (
            PATH,
            channel,
            bird,
            channel,
        )
        if not os.path.isfile(f"{fnbase}.json"):
            LOG.info("Missing %s.json", fnbase)
            continue
        with open(f"{fnbase}.json", encoding="utf8") as fp:
            info = json.load(fp)
        ts = datetime.datetime.strptime(info["meta"]["valid"], ISO).replace(
            tzinfo=ZoneInfo("UTC")
        )
        if (valid - ts) > datetime.timedelta(hours=6):
            continue
        # step 1
        fn = f"{tmpname}_{bird}.tif"
        tiles.append(fn)
        cmd = (
            "gdalwarp -q -s_srs '%s' -t_srs 'EPSG:4326' -te %s %s %s %s "
            f"-tr 0.04 0.04 -of GTiff %s.png {fn}"
        ) % (
            info["meta"]["proj4str"],
            -140 if bird in [17, 18] else -113.1,
            22,
            -112.9 if bird in [17, 18] else -60,
            52,
            fnbase,
        )
        call(cmd)

    if not tiles:
        LOG.warning("No data found for tiling...")
        return
    cmd = (
        "gdal_merge.py -q -o %s.tif -of GTiff -ps 0.04 0.04 "
        "-ul_lr -130 52 -60 22 %s"
    ) % (tmpname, " ".join(tiles))
    call(cmd)

    # GIS/sat/conus_goes_vis4km_1045.tif
    cmd = (
        "pqinsert -i -p 'gis ac %s gis/images/4326/sat/conus_goes_%s4km.tif "
        "GIS/sat/conus_goes_%s4km_%s.tif tif' %s.tif"
    ) % (
        valid.strftime("%Y%m%d%H%M"),
        LOOKUP[channel],
        LOOKUP[channel],
        valid.strftime("%H%M"),
        tmpname,
    )
    call(cmd)


def main(argv):
    """Go Main Go."""
    valid = utc(*[int(a) for a in argv[1:6]])
    LOG.info("valid is set to: %s", valid)
    for channel in [2, 9, 13]:
        with tempfile.NamedTemporaryFile() as tmpfd:
            try:
                run(valid, channel, tmpfd.name)
            except Exception as exp:
                LOG.exception(exp)
        for part in ["", "_16", "_17", "_18"]:
            fn = f"{tmpfd.name}{part}.tif"
            if not os.path.isfile(fn):
                continue
            os.unlink(fn)


if __name__ == "__main__":
    main(sys.argv)
