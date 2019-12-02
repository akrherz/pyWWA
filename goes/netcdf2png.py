"""Convert the Unidata GOES product netcdf files to IEM PNG.

The devil is in the details here :)

"""
import datetime
import subprocess
import traceback
import tempfile
import os
import sys

import inotify.adapters
from PIL import Image
import numpy as np
from netCDF4 import Dataset
from pyiem.util import logger

LOG = logger()
DIRPATH = "/mnt/goestemp"
RAMPS = {}
SECTORS = {
    "C": "CONUS",
    "HI": "Hawaii",
    "PR": "PuertoRico",
    "AK": "Alaska",
    "F": "FullDisk",
    "M1": "Mesoscale-1",
    "M2": "Mesoscale-2",
}


def build_ramps():
    """Make our color ramps."""
    # WV
    RAMPS["wv"] = np.loadtxt("wvramp.txt")
    RAMPS["ir"] = np.loadtxt("irramp.txt")
    # This is matplotlib's ramp
    RAMPS["greys"] = np.loadtxt("greysramp.txt")


def make_image(nc):
    """Generate a PIL Image.

    Thanks to help/code from College of Dupage, we have a fighting chance
    understanding how we should convert the raw netcdf data into PNG with a
    good color ramp for the data provided by the channel.  Some notes here:

    - The matplotlib Greys_r ramp is not exactly linear.

    """
    channel = nc.channel_id
    ncvar = nc.variables["Sectorized_CMI"]
    data = ncvar[:]
    valid_min = ncvar.valid_min * ncvar.scale_factor + ncvar.add_offset
    valid_max = ncvar.valid_max * ncvar.scale_factor + ncvar.add_offset

    # default grayscale
    colors = RAMPS["greys"][:, 1:].astype("i").ravel()
    if channel in [1, 2, 3, 4, 5, 6]:
        # guidance is to take square root of data and apply grayscale
        imgdata = data ** 0.5 * 255
    elif channel in [7]:
        imgdata = np.where(data < 242, (418.0 - data), ((330.0 - data) * 2.0))
    elif channel in [8, 9, 10]:
        # Convert to Celsius?
        imgdata = np.digitize(data - 273.15, RAMPS["wv"][:, 0])
        colors = RAMPS["wv"][:, 1:].astype("i").ravel()
    elif channel in [13, 14, 15]:
        imgdata = np.where(data < 242, (418.0 - data), ((330.0 - data) * 2.0))
        colors = RAMPS["ir"][:, 1:].astype("i").ravel()
    else:
        # scale from min to max
        imgdata = (data - valid_min) / (valid_max - valid_min) * 255.0
    png = Image.fromarray(imgdata.astype("u1"))
    png.putpalette(list(colors))
    return png


def get_sector(fn):
    """Figure out the label to associate with this file, sigh."""
    # NB nc.source_scene is not the region of view
    tokens = fn.split("-")
    s = tokens[2].replace("CMIP", "")
    return SECTORS.get(s, s)


def process(ncfn):
    """Actually process this file!"""
    if not os.path.isfile(ncfn):
        return
    sector = get_sector(ncfn)
    with Dataset(ncfn) as nc:
        valid = datetime.datetime.strptime(nc.start_date_time, "%Y%j%H%M%S")
        bird = nc.satellite_id
        channel = nc.channel_id
        lon_0 = nc.satellite_longitude
        h = nc.variables["fixedgrid_projection"].perspective_point_height
        swa = nc.variables["fixedgrid_projection"].sweep_angle_axis
        x0 = nc.variables["x"][0]
        dx = nc.variables["x"].scale_factor / 1e6 * h
        y0 = nc.variables["y"][0] / 1e6 * h
        x0 = nc.variables["x"][0] / 1e6 * h
        dy = nc.variables["y"].scale_factor / 1e6 * h
        png = make_image(nc)
        tmpfd = tempfile.NamedTemporaryFile(delete=False)
        png.save(tmpfd, format="PNG")
        tmpfd.close()

        pqstr = (
            "gis c %s gis/images/GOES/%s/channel%02i/%s_C%02i.png %s png"
        ) % (
            valid.strftime("%Y%m%d%H%M"),
            sector,
            channel,
            bird,
            channel,
            valid.strftime("%Y%m%d%H%M%S"),
        )
        subprocess.call(
            "pqinsert -i -p '%s' %s" % (pqstr, tmpfd.name), shell=True
        )
        os.unlink(tmpfd.name)
        tmpfd = tempfile.NamedTemporaryFile(mode="w", delete=False)
        args = [dx, 0, 0, dy, x0, y0]
        tmpfd.write("\n".join([str(s) for s in args]))
        tmpfd.close()
        subprocess.call(
            "pqinsert -i -p '%s' %s"
            % (pqstr.replace("png", "wld"), tmpfd.name),
            shell=True,
        )
        with open(tmpfd.name, "w") as fh:
            fh.write(
                (
                    "PROJECTION\n"
                    "proj=geos\n"
                    "h=%s\n"
                    "lon_0=%s\n"
                    "sweep=%s\n"
                    "END\n"
                )
                % (h, lon_0, swa)
            )
        subprocess.call(
            "pqinsert -i -p '%s' %s"
            % (pqstr.replace("png", "msinc"), tmpfd.name),
            shell=True,
        )
        os.unlink(tmpfd.name)

    # np.digitize(data, bins)


def main(argv):
    """Go Main Go."""
    # Shard by bird, either G16 or G17
    bird = "_%s_" % (argv[1],)
    inotif = inotify.adapters.Inotify()
    inotif.add_watch(DIRPATH)
    try:
        for event in inotif.event_gen():
            if event is None:
                continue
            (_header, type_names, watch_path, fn) = event
            # Careful here, it seems like we get the IN_CLOSE_NOWRITE when
            # we double back with the netcdf read statement above closing.
            if "IN_CLOSE_WRITE" not in type_names:
                continue
            # LOG.debug("fn: %s type_names: %s", fn, type_names)
            if not fn.endswith(".nc") or fn.find(bird) == -1:
                continue
            ncfn = "%s/%s" % (watch_path, fn)
            try:
                # LOG.debug("Processing %s", ncfn)
                process(ncfn)
                if os.path.isfile(ncfn):
                    os.unlink(ncfn)
            except Exception as exp:
                with open("%s.error" % (ncfn,), "w") as fp:
                    fp.write(str(exp) + "\n")
                    traceback.print_exc(file=fp)
    finally:
        inotif.remove_watch(DIRPATH)


if __name__ == "__main__":
    build_ramps()
    main(sys.argv)
