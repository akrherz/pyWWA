"""Do the rotation action that some products need."""
import gzip
import os
import sys

BASE = "/mesonet/ldmdata/"


def main(argv):
    """Do SOmething"""
    data = sys.stdin.buffer.read()
    fnbase = argv[1]
    fmt = argv[2]

    dirname = f"{BASE}/{os.path.dirname(fnbase)}"
    if not os.path.isdir(dirname):
        os.makedirs(dirname)

    if fmt == "tif.Z":
        for i in range(10, -1, -1):
            oldfp = f"{BASE}/{fnbase}{i}.{fmt}"
            newfp = f"{BASE}/{fnbase}{i + 1}.{fmt}"
            if os.path.isfile(oldfp):
                os.rename(oldfp, newfp)

        with open(f"{BASE}/{fnbase}0.{fmt}", "wb") as fh:
            fh.write(data)

        with gzip.open(f"{BASE}/{fnbase}0.{fmt}", "rb") as fh:
            data = fh.read()
        fmt = "tif"

    for i in range(10, -1, -1):
        oldfp = f"{BASE}/{fnbase}{i}.{fmt}"
        newfp = f"{BASE}/{fnbase}{i + 1}.{fmt}"
        if os.path.isfile(oldfp):
            os.rename(oldfp, newfp)

    with open(f"{BASE}/{fnbase}0.{fmt}", "wb") as fh:
        fh.write(data)


if __name__ == "__main__":
    main(sys.argv)
