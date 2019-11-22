"""Produce a greys cmap."""
from pyiem.plot.use_agg import plt


def main():
    """Go Main Go."""
    cmap = plt.get_cmap("Greys_r")
    for i in range(256):
        c = cmap(i / 255.0)
        print("%s %.0f %.0f %.0f" % (i, c[0] * 255, c[1] * 255, c[2] * 255))


if __name__ == "__main__":
    main()
