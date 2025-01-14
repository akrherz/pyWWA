"""Convert noaaport GINI imagery into GIS PNGs

I convert raw GINI noaaport imagery into geo-referenced PNG files both in the
'native' projection and 4326.
"""

import json
import os
import subprocess
import sys
import tempfile
from io import BytesIO

import click
import numpy as np
from PIL import Image
from pyiem.nws import gini

from pywwa import LOG, common

WORLDFILE_FORMAT = "%(dx).3f\n0.0\n0.0\n-%(dy).3f\n%(x0).3f\n%(y1).3f"


def process_input():
    """
    Process what was provided to use by LDM on stdin
    @return GINIZFile instance
    """
    cstr = BytesIO()
    payload = getattr(sys.stdin, "buffer", sys.stdin).read()
    if len(payload) < 100:
        return None
    cstr.write(payload)
    cstr.seek(0)
    sat = gini.GINIZFile(cstr)
    return sat


def do_legacy_ir(sat, tmpfn):
    """since some are unable to process non-grayscale"""
    png = Image.fromarray(np.array(sat.data[:-1, :], np.uint8))
    png.save(f"{tmpfn}.png")

    # World File

    with open(f"{tmpfn}.wld", "w") as fh:
        fh.write(WORLDFILE_FORMAT % sat.metadata)

    tstamp = sat.metadata["valid"].strftime("%Y%m%d%H%M")
    cmd = [
        "pqinsert",
        "-i",
        "-p",
        f"gis c {tstamp} gis/images/awips{sat.awips_grid()}/"
        f"{sat.current_filename().replace('.png', '_gray.png')} GIS/sat/"
        f"awips{sat.awips.grid()}/{sat.archive_filename()} png",
        f"{tmpfn}.png",
    ]
    subprocess.call(cmd)

    cmd = [
        "pqinsert",
        "-i",
        "-p",
        f"gis c {tstamp} gis/images/awips{sat.awips_grid()}/"
        f"{sat.current_filename().replace('.png', '_gray.wld')} GIS/sat/"
        f"awips{sat.awips.grid()}/{sat.archive_filename()} wld",
        f"{tmpfn}.wld",
    ]
    subprocess.call(cmd)


def write_gispng(sat, tmpfn):
    """
    Write PNG file with associated .wld file.
    """

    if sat.get_channel() == "IR":
        do_legacy_ir(sat, tmpfn)
        # Make enhanced version
        png = Image.fromarray(np.array(sat.data[:-1, :], np.uint8))
        png.putpalette(tuple(gini.get_ir_ramp().ravel()))
    else:
        png = Image.fromarray(np.array(sat.data[:-1, :], np.uint8))
    png.save(f"{tmpfn}.png")

    # World File
    with open(f"{tmpfn}.wld", "w") as fh:
        fh.write(WORLDFILE_FORMAT % sat.metadata)

    tstamp = sat.metadata["valid"].strftime("%Y%m%d%H%M")
    cmd = [
        "pqinsert",
        "-i",
        "-p",
        f"gis {get_ldm_routes(sat)} {tstamp} gis/images/"
        f"awips{sat.awips_grid()}/{sat.current_filename()} "
        f"GIS/sat/awips{sat.awips_grid()}/{sat.archive_filename()} png",
        f"{tmpfn}.png",
    ]
    subprocess.call(cmd)

    cmd[3] = cmd[3].replace("png", "wld")
    cmd[4] = cmd[4].replace("png", "wld")
    subprocess.call(cmd)


def write_metadata(sat, tmpfn):
    """
    Write a JSON formatted metadata file
    """
    metadata = {"meta": {}}
    tstamp = sat.metadata["valid"].strftime("%Y-%m-%dT%H:%M:%SZ")
    metadata["meta"]["valid"] = tstamp
    metadata["meta"]["awips_grid"] = sat.awips_grid()
    metadata["meta"]["bird"] = sat.get_bird()
    metadata["meta"]["archive_filename"] = sat.archive_filename()
    metafp = f"{tmpfn}.json"
    with open(metafp, "w") as fh:
        json.dump(metadata, fh)

    tstamp = sat.metadata["valid"].strftime("%Y%m%d%H%M")
    cmd = [
        "pqinsert",
        "-i",
        "-p",
        f"gis c {tstamp} gis/images/awips{sat.awips_grid()}/"
        f"{sat.current_filename().replace('png', 'json')} "
        f"GIS/sat/{sat.archive_filename().replace('png', 'json')} json",
        f"{tmpfn}.json",
    ]
    subprocess.call(cmd)
    os.unlink(f"{tmpfn}.json")


def write_mapserver_metadata(sat, tmpfn, epsg):
    """Write out and pqinsert a metadata file that mapserver can use to
    provide WMS metadata."""
    metafn = f"{tmpfn}.txt"
    with open(metafn, "w") as fh:
        fh.write(
            f"""
  METADATA
    "wms_title" "{sat.get_bird()} {sat.get_sector()} {sat.get_channel()} \
 valid {sat.metadata["valid"]:%Y-%m-%dT%H:%M:%SZ} UTC"
    "wms_srs"   "EPSG:4326 EPSG:26915 EPSG:900913 EPSG:3857"
    "wms_extent" "-126 24 -66 50"
  END
"""
        )
    cmd = [
        "pqinsert",
        "-i",
        "-p",
        f"gis c {sat.metadata['valid']:%Y%m%d%H%M} "
        f"gis/images/{epsg}/goes/"
        f"{sat.current_filename().replace('png', 'msinc')} bogus msinc",
        metafn,
    ]
    subprocess.call(cmd)
    os.unlink(metafn)


def write_metadata_epsg(sat, tmpfn, epsg):
    """
    Write a JSON formatted metadata file
    """
    metadata = {"meta": {}}
    tstamp = sat.metadata["valid"].strftime("%Y-%m-%dT%H:%M:%SZ")
    metadata["meta"]["valid"] = tstamp
    metadata["meta"]["epsg"] = epsg
    metadata["meta"]["bird"] = sat.get_bird()
    metadata["meta"]["archive_filename"] = sat.archive_filename()
    metafp = f"{tmpfn}_{epsg}.json"
    with open(metafp, "w") as fh:
        json.dump(metadata, fh)

    tstamp = sat.metadata["valid"].strftime("%Y%m%d%H%M")
    cmd = [
        "pqinsert",
        "-i",
        "-p",
        f"gis c {tstamp} gis/images/{epsg}/goes/"
        f"{sat.current_filename().replace('png', 'json')} bogus json",
        f"{tmpfn}_{epsg}.json",
    ]
    subprocess.call(cmd)
    os.unlink(f"{tmpfn}_{epsg}.json")


def get_ldm_routes(sat):
    """
    Figure out if this product should be routed to current or archived folders
    """
    minutes = (common.utcnow() - sat.metadata["valid"]).seconds / 60.0
    if minutes > 120:
        return "a"
    return "ac"


def gdalwarp(sat, tmpfn, epsg):
    """
    Convert imagery into some EPSG projection, typically 4326
    """
    cmd = [
        "gdalwarp",
        "-q",
        "-of",
        "GTiff",
        "-co",
        "TFW=YES",
        "-s_srs",
        sat.metadata["proj"].srs,
        "-t_srs",
        f"EPSG:{epsg}",
        f"{tmpfn}.png",
        f"{tmpfn}_{epsg}.tif",
    ]
    subprocess.call(cmd)

    # Convert file back to PNG for use and archival (smaller file)
    cmd = [
        "convert",
        "-quiet",
        "-define",
        "PNG:preserve-colormap",
        f"{tmpfn}_{epsg}.tif",
        f"{tmpfn}_4326.png",
    ]
    proc = subprocess.Popen(
        cmd, stderr=subprocess.PIPE, stdout=subprocess.PIPE
    )
    output = proc.stderr.read()
    if output != b"":
        LOG.error("gdalwarp() convert error message: %s", output)
    os.unlink(f"{tmpfn}_{epsg}.tif")

    tstamp = sat.metadata["valid"].strftime("%Y%m%d%H%M")
    cmd = [
        "pqinsert",
        "-i",
        "-p",
        f"gis c {tstamp} gis/images/{epsg}/goes/"
        f"{sat.current_filename().replace('png', 'wld')} bogus wld",
        f"{tmpfn}_{epsg}.tfw",
    ]
    subprocess.call(cmd)

    cmd[3] = cmd[3].replace("wld", "png")
    cmd[4] = cmd[4].replace("wld", "png")
    subprocess.call(cmd)

    os.unlink(f"{tmpfn}_{epsg}.png")
    os.unlink(f"{tmpfn}_{epsg}.tfw")


def cleanup(tmpfn):
    """
    Pickup after ourself
    """
    for suffix in ["png", "wld", "tfw"]:
        if os.path.isfile(f"{tmpfn}.{suffix}"):
            os.unlink(f"{tmpfn}.{suffix}")


def workflow():
    """Go."""
    sat = process_input()
    if sat is None:
        LOG.info("ABORT: No data found!")
        return
    LOG.info("Processed archive file: %s", sat.archive_filename())
    if sat.awips_grid() is None:
        LOG.info("ABORT: Unknown awips grid!")
        return

    # Generate a temporary filename to use for our work
    with tempfile.NamedTemporaryFile() as tmpfd:
        # Write PNG
        write_gispng(sat, tmpfd.name)

        if get_ldm_routes(sat) == "ac":
            # Write JSON metadata
            write_metadata(sat, tmpfd.name)
            # Write JSON metadata for 4326 file
            write_metadata_epsg(sat, tmpfd.name, 4326)
            # write mapserver include metadatab
            write_mapserver_metadata(sat, tmpfd.name, 4326)
            # Warp the file into 4326
            gdalwarp(sat, tmpfd.name, 4326)
        # cleanup after ourself
        cleanup(tmpfd.name)

    LOG.info("Done!")


@click.command(help=__doc__)
@common.init
@common.disable_xmpp
def main(*args, **kwargs):
    """workflow"""
    workflow()
