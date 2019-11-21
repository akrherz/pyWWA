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


def process(path, fn):
    """Actually process this file!"""
    ncfn = "%s/%s" % (path, fn)
    with Dataset(ncfn) as nc:
        data = nc.variables["Sectorized_CMI"][:]
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
        if nc.channel_id < 5:
            # guidance is to take square root of data and scale it to 255
            imgdata = (data ** 0.5 * 255).astype("u1")
            png = Image.fromarray(imgdata)
            png.putpalette(list(np.repeat(range(256), 3)))
            png.save("/tmp/daryl.png")

            pqstr = (
                "gis c %s gis/images/GOES/%s/channel%02i/%s_C%02i.png %s png"
            ) % (
                valid.strftime("%Y%m%d%H%M"),
                nc.source_scene,
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
    main()
