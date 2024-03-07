"""Make a file on disk appear more noaaport-ish."""

import sys

from pyiem.util import noaaport_text


def main(argv):
    """Do Main Things"""
    fn = argv[1]
    with open(fn, "rb") as fh:
        data = noaaport_text(fh.read().decode("ascii"))
    with open(fn, "w", encoding="ascii") as fh:
        fh.write(data)


if __name__ == "__main__":
    main(sys.argv)
