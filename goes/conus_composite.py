"""Generate some CONUS composites like we had back in the day.

1. gdalwarp: convert East and West into CONUS EPSG:4326 at desired res ~4km
   appears CONUS goes west goes to just -112 east?
2. gdal_merge.py: combine these two with the west file last (on top)
"""
import json
import os
import sys
import tempfile
import subprocess

from pyiem.util import logger, utc

LOG = logger()
PATH = "/home/meteor_ldm/data/gis/images/GOES/conus"
LOOKUP = {2: "vis", 9: "wv", 13: "ir"}


def filtermsg(buf):
    """Less is more"""
    text = buf.decode('ascii')
    return 'Point outside of projection' in text


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
            LOG.info(cmd)
            LOG.info(stdout)
    if stderr != b"":
        if not filtermsg(stderr):
            LOG.info(cmd)
            LOG.info(stderr)


def run(valid, channel, tmpname):
    """Do work."""
    for bird in [16, 17]:
        fnbase = "%s/channel%02i/GOES-%s_C%02i" % (
            PATH,
            channel,
            bird,
            channel,
        )
        info = json.load(open(fnbase + ".json"))
        # step 1
        cmd = (
            "gdalwarp -q -s_srs '%s' -t_srs 'EPSG:4326' -te %s %s %s %s "
            "-tr 0.04 0.04 -of GTiff %s.png %s_%s.tif"
        ) % (
            info["meta"]["proj4str"],
            -140 if bird == 17 else -113.1,
            22,
            -112.9 if bird == 17 else -60,
            52,
            fnbase,
            tmpname,
            bird,
        )
        call(cmd)

    cmd = (
        "gdal_merge.py -q -o %s.tif -of GTiff -ps 0.04 0.04 "
        "-ul_lr -130 52 -60 22 %s_17.tif %s_16.tif"
    ) % (tmpname, tmpname, tmpname)
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
    LOG.debug("valid is set to: %s", valid)
    for channel in [2, 9, 13]:
        with tempfile.NamedTemporaryFile() as tmpfd:
            try:
                run(valid, channel, tmpfd.name)
            except Exception as exp:
                LOG.exception(exp)
            finally:
                for part in ["", "_16", "_17"]:
                    fn = "%s%s.tif" % (tmpfd.name, part)
                    if not os.path.isfile(fn):
                        continue
                    os.unlink(fn)


if __name__ == "__main__":
    main(sys.argv)
