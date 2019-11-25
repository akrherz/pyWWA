"""Convert the Unidata GOES product netcdf files to IEM PNG.

The devil is in the details here :)

"""
import datetime
import subprocess
import traceback
import inotify.adapters

from PIL import Image
import numpy as np
from netCDF4 import Dataset

DIRPATH = "/mnt/goestemp"
RAMPS = {}
SECTORS = {
    "C": "CONUS",
    "HI": "Hawaii",
    "PR": "Puerto Rico",
    "AK": "Alaska",
    "F": "FullDisk",
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


def process(path, fn):
    """Actually process this file!"""
    ncfn = "%s/%s" % (path, fn)
    sector = get_sector(fn)
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
        png.save("/tmp/daryl.png")

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
            "pqinsert -i -p '%s' /tmp/daryl.png" % (pqstr,), shell=True
        )
        with open("/tmp/daryl.wld", "w") as fh:
            args = [dx, 0, 0, dy, x0, y0]
            fh.write("\n".join([str(s) for s in args]))
        subprocess.call(
            "pqinsert -i -p '%s' /tmp/daryl.wld"
            % (pqstr.replace("png", "wld"),),
            shell=True,
        )
        with open("/tmp/daryl.msinc", "w") as fh:
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
            "pqinsert -i -p '%s' /tmp/daryl.msinc"
            % (pqstr.replace("png", "msinc"),),
            shell=True,
        )

    # np.digitize(data, bins)


def main():
    """Go Main Go."""
    inotif = inotify.adapters.Inotify()
    inotif.add_watch(DIRPATH)
    try:
        for event in inotif.event_gen():
            if event is None:
                continue
            (_header, type_names, watch_path, fn) = event
            if "IN_CLOSE_WRITE" not in type_names:
                continue
            if not fn.endswith(".nc"):
                continue
            try:
                process(watch_path, fn)
                subprocess.call("rm -f %s/%s" % (watch_path, fn), shell=True)
            except Exception as exp:
                with open("%s/%s.error" % (watch_path, fn), "w") as fp:
                    fp.write(str(exp) + "\n")
                    traceback.print_exc(file=fp)
    finally:
        inotif.remove_watch(DIRPATH)


if __name__ == "__main__":
    build_ramps()
    main()
