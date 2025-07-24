"""NLDN from Vaisala, TSV format."""

import math

import click
from pyiem.util import utc

from pywwa import LOG, common
from pywwa.database import get_database
from pywwa.ldm import bridge


def process_data(txn, line: str):
    """The real processor of the raw data, fun!"""
    if line == "":
        return None
    tokens = line.split("\t")
    if len(tokens) != 25:
        LOG.info("Invalid NLDN line: `%s`", line)
        return
    # Only care about CG
    if tokens[21] != "0":
        return
    """
    [0] The UALF record type (it will always be 0 for these types of records).
    [1] Year, 1970 to 2032.
    [2] Month, with January as 1 and December as 12.
    [3] Day of the month, 1 to 31.
    [4] Hour, 0 to 23.
    [5] Minute, 0 to 59.
    [6] Second, 0 to 60.
    [7] Nanosecond, 0 to 999999999.
    [8] Latitude of the calculated location in decimal degrees,
        to 4 decimal places, -90.0 to 90.0.
    [9] Longitude of the calculated location in decimal degrees,
        to 4 decimal places, -180.0 to 180.0.
    [10] Estimated peak current in kiloamps, -9999 to 9999.
    [11] Multiplicity for flash data (1 to 99) or 0 for strokes.
    [12] Number of sensors participating in the solution, 2 to 99.
    [13] Degrees of freedom when optimizing location, 0 to 99.
    [14] The error ellipse angle as a clockwise bearing from 0 degrees north,
         0 to 180.0 degrees.
    [15] The error ellipse semi-major axis length in kilometers,
         0.00 to 50.00 km.
    [16] The error ellipse semi-minor axis length in kilometers, 0 to 50.0 km.
    [17] Chi-squared value from location optimization, 0 to 999.99.
    [18] Rise time of the waveform in microseconds, 0 to 99.9.
    [19] Peak-to-zero time of the waveform in microseconds, 0 to 999.9.
    [20] Maximum rate-of-rise of the waveform in kA/usec
         (will be a negative rate if discharge is negative), -999.9 to 999.9.
    [21] Cloud indicator, 1 if Cloud-to-cloud discharge, 0 for Cloud-to-ground.
    [22] Angle indicator, 1 if sensor angle data used to compute position,
         0 otherwise.
    [23] Signal indicator, 1 if sensor signal data used to compute position,
         0 otherwise.
    [24] Timing indicator, 1 if sensor timing data used to compute position,
         0 otherwise.
    """
    nanoseconds = int(tokens[7])
    valid = utc(*[int(x) for x in tokens[1:7]]).replace(
        microsecond=nanoseconds // 1000
    )
    table = f"nldn{valid:%Y_%m}"
    # Based on what readNLDNSocket.tcl had
    # The binary decoder maybe should have been dividing this by 10?
    eccentricity = 10.0 * math.sqrt(
        1.0 - (float(tokens[16]) ** 2 / float(tokens[15]) ** 2)
    )
    txn.execute(
        f"INSERT into {table} (valid, geom, signal, multiplicity, "
        "axis, eccentricity, ellipse, chisqr) VALUES (%s, "
        "ST_Point(%s, %s, 4326), %s, %s, %s, %s, %s, %s)",
        (
            valid,
            float(tokens[9]),
            float(tokens[8]),
            float(tokens[10]),  # verified
            int(tokens[11]),  # verified
            int(float(tokens[15])),  # verified
            int(eccentricity),  # verified
            int(float(tokens[14])),  # verified
            int(float(tokens[17])),  # verified
        ),
    )

    return


@click.command(help=__doc__)
@common.init
@common.disable_xmpp
def main(*args, **kwargs):
    """Go Main"""
    bridge(
        process_data,
        isbinary=False,
        product_end=b"\r\n",
        dbpool=get_database("nldn"),
    )
