"""Generate a color ramp image, please."""
import sys

import netCDF4
import numpy as np
import pandas as pd
from PIL import Image, ImageDraw, ImageFont

CHANNEL_LOOKUP = {
    1: "greys",
    2: "greys",
    3: "greys",
    4: "greys",
    5: "greys",
    6: "greys",
    7: "greys",
    8: "wv",
    9: "wv",
    10: "wv",
    11: "greys",
    12: "greys",
    13: "ir",
    14: "ir",
    15: "ir",
    16: "greys",
}


def compute_range_units(channel):
    """Use example netcdf files locally."""
    nc = netCDF4.Dataset(
        f"OR_ABI-L2-CMIPC-M6C{channel:02.0f}_G16_"
        "s20193251636150_e20193251636150_c20193251636150.nc"
    )
    ncvar = nc.variables["Sectorized_CMI"]
    valid_min = ncvar.valid_min * ncvar.scale_factor + ncvar.add_offset
    valid_max = ncvar.valid_max * ncvar.scale_factor + ncvar.add_offset
    units = ncvar.units
    nc.close()
    return valid_min, valid_max, units.replace("kelvin", "K")


def main(argv):
    """Go Main Go."""
    channel = int(argv[1])
    df = pd.read_csv(
        f"{CHANNEL_LOOKUP[channel]}ramp.txt",
        names=["value", "r", "g", "b"],
        sep=" ",
    )
    vmin, vmax, units = compute_range_units(channel)
    data = np.linspace(vmin, vmax, 256)
    # We are using the netcdf2png code to figure out how values are mapped
    # into colors.
    if channel in [1, 2]:
        imgdata = np.where(
            data < 0.91,
            np.sqrt(data),
            0.9539 + (1.0 - (1.16 - data) / 0.25) * 0.0461,
        )
        imgdata = np.where(imgdata > 1, 1, imgdata) * 255
    elif channel in [3, 4, 5, 6]:
        # guidance is to take square root of data and apply grayscale
        imgdata = data**0.5 * 255
    elif channel in [7]:
        imgdata = np.where(data < 242, (418.0 - data), ((330.0 - data) * 2.0))
    elif channel in [8, 9, 10]:
        # Convert to Celsius?
        imgdata = np.digitize(data - 273.15, df["value"].values)
    elif channel in [13, 14, 15]:
        imgdata = np.where(data < 242, (418.0 - data), ((330.0 - data) * 2.0))
    else:
        # scale from min to max
        imgdata = (data - vmin) / (vmax - vmin) * 255.0

    # OK, we have data, which is our raw values and imgdata, which is how the
    # data maps to a 0-255 value.

    # Create a 256x30 image
    img = np.zeros((30, 256), dtype=np.uint8)
    img[:, :] = imgdata.astype(np.uint8)
    img[15:, :] = 0

    font = ImageFont.truetype(
        "/usr/share/fonts/liberation-mono/LiberationMono-Regular.ttf",
        10,
    )
    png = Image.fromarray(img, mode="L")
    colors = df[["r", "g", "b"]].values.ravel()
    # last index needs to be white
    colors[-3:] = 255
    png.putpalette(list(colors))
    draw = ImageDraw.Draw(png)

    # sample every 50 rows in df
    for x, row in df.iloc[50::50].iterrows():
        print(imgdata[x], data[x], row["value"])
        txt = f"{data[x]:.1f}"
        draw.line([x, 17, x, 10], fill=255)
        draw.text((x - 15, 18), txt, fill=255, font=font)

    draw.text((0, 17), units, fill=255, font=font)

    png.save(f"goes_c{channel:02.0f}.png")


if __name__ == "__main__":
    main(sys.argv)
