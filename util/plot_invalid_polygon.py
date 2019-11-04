"""Make a simple plot to illustrate an invalid polygon"""
import matplotlib.pyplot as plt
from pyiem.nws.vtec import VTEC_PHENOMENA, VTEC_SIGNIFICANCE

X = [-90.92, -91.02, -90.93, -91.02, -90.92]
Y = [30.4, 30.4, 30.5, 30.5, 30.4]
WFO = "LIX"
ETN = 8
PHENOMENA = "FL"
SIGNIFICANCE = "W"
YEAR = "2018"


def main():
    """Go Main Go"""
    (fig, ax) = plt.subplots(1, 1)
    for i in range(len(X) - 1):
        ax.arrow(
            X[i],
            Y[i],
            X[i + 1] - X[i],
            Y[i + 1] - Y[i],
            head_width=0.003,
            width=0.0005,
        )
        ax.text(X[i] + 0.005, Y[i] + 0.005, str(i + 1), va="top")

    ax.set_xlim(min(X) - 0.02, max(X) + 0.02)
    ax.set_ylim(min(Y) - 0.02, max(Y) + 0.02)
    ax.set_ylabel(r"Latitude [$^\circ$N]")
    ax.set_xlabel(r"Longitude [$^\circ$E]")
    ax.set_title(
        "%s %s %s %s (%s.%s) #%s Polygon"
        % (
            YEAR,
            WFO,
            VTEC_PHENOMENA[PHENOMENA],
            VTEC_SIGNIFICANCE[SIGNIFICANCE],
            PHENOMENA,
            SIGNIFICANCE,
            ETN,
        )
    )
    ax.grid(True)
    for tick in ax.get_xticklabels():
        tick.set_rotation(45)
    ax.set_position([0.2, 0.2, 0.7, 0.7])
    fn = "/tmp/%s_%s_%s_%s_%s.png" % (YEAR, WFO, PHENOMENA, SIGNIFICANCE, ETN)
    print("writing %s" % (fn,))
    fig.savefig(fn)


if __name__ == "__main__":
    main()
