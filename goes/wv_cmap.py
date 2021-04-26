"""Hackery."""
import matplotlib.colors as mpcolors

# Make colormap (based off of
# http://cimss.ssec.wisc.edu/goes/visit/water_vapor_enhancement.html):
colors = [
    "#000000",
    "#00FFFF",
    "#006C00",
    "#FFFFFF",
    "#0000A6",
    "#FFFF00",
    "#FF0000",
    "#000000",
    "#000000",
]
cints = [-109.5, -109.0, -75.0, -47.0, -30.0, -15.5, 0.0, 0.5, 54.5]
cints = [(float(c + 109.5) / float(54.5 + 109.5)) for c in cints]
colorList = []
for i in list(range(0, len(cints))):
    colorList.append((cints[i], colors[i]))
cmap = mpcolors.LinearSegmentedColormap.from_list("mycmap", colorList)
for i in range(256):
    val = -109.5 + (i / 255) * (54.5 + 109.5)
    c = cmap(i / 255.0)
    print("%.2f %.0f %.0f %.0f" % (val, c[0] * 255, c[1] * 255, c[2] * 255))
# vmax = 54.4
# vmin = -109.
