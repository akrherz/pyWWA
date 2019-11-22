"""Hackery."""
import matplotlib.colors as mpcolors
import numpy as np

colors = [
    (0, 0, 0),
    (120, 120, 120),
    (3, 3, 3),
    (90, 90, 90),
    (91, 91, 91),
    (100, 100, 100),
    (103, 103, 103),
    (248, 248, 248),
    (250, 250, 250),
    (191, 0, 255),
    (191, 0, 255),
    (0, 0, 255),
    (0, 12, 242),
    (0, 255, 0),
    (25, 255, 0),
    (255, 255, 0),
    (255, 229, 0),
    (255, 0, 0),
    (229, 0, 0),
    (0, 0, 0),
    (20, 20, 20),
    (255, 255, 255),
    (255, 255, 255),
    (0, 0, 0),
]
colors = ["#%02x%02x%02x" % x for x in colors]
cints = [
    0.0,
    60.0,
    61.0,
    90.0,
    91.0,
    100.0,
    101.0,
    144.0,
    145.0,
    154.0,
    155.0,
    170.0,
    171.0,
    190.0,
    191.0,
    200.0,
    201.0,
    210.0,
    211.0,
    220.0,
    221.0,
    245.0,
    246.0,
    255.0,
]
cints = [float(c / 255.0) for c in cints]
colorList = []
for i in list(range(0, len(cints))):
    colorList.append((cints[i], colors[i]))
cmap = mpcolors.LinearSegmentedColormap.from_list("mycmap", colorList)

for i in range(256):
    c = cmap(i / 255.0)
    print("%.2f %.0f %.0f %.0f" % (i, c[0] * 255, c[1] * 255, c[2] * 255))
# vmax = 54.4
# vmin = -109.
