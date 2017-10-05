"""Make a simple plot to illustrate an invalid polygon"""
import matplotlib.pyplot as plt

X = [-106.21, -106.2, -106.26, -106.38, -106.36, -106.26, -106.21]
Y = [35.47, 35.45, 35.49, 35.56, 35.53, 35.51, 35.47]


def main():
    """Go Main Go"""
    (fig, ax) = plt.subplots(1, 1)
    for i in range(len(X) - 1):
        ax.arrow(X[i], Y[i], X[i+1] - X[i], Y[i+1] - Y[i], head_width=0.003,
                 width=0.0005)
        ax.text(X[i] + 0.005, Y[i] + 0.005, str(i+1), va='top')

    ax.set_xlim(min(X) - 0.02, max(X) + 0.02)
    ax.set_ylim(min(Y) - 0.02, max(Y) + 0.02)
    ax.set_ylabel(r"Latitude [$^\circ$N]")
    ax.set_xlabel(r"Longitude [$^\circ$E]")
    ax.set_title("2017 ABQ Flood Warning (FL.W) #4 Polygon")
    ax.grid(True)
    for tick in ax.get_xticklabels():
        tick.set_rotation(45)
    ax.set_position([0.2, 0.2, 0.7, 0.7])
    fig.savefig('test.png')


if __name__ == '__main__':
    main()
