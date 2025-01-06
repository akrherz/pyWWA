"""Do the rotation action that some products need."""

import gzip
import os
import sys
from io import BytesIO

BASE = "/mesonet/ldmdata/"


def handler(fnbase: str, fmt: str, bio: BytesIO):
    """Do things."""
    if fmt == "tif.Z":
        for i in range(10, -1, -1):
            oldfp = f"{BASE}/{fnbase}{i}.{fmt}"
            newfp = f"{BASE}/{fnbase}{i + 1}.{fmt}"
            if os.path.isfile(oldfp):
                os.rename(oldfp, newfp)

        # Write out the compressed version verbatim
        with open(f"{BASE}/{fnbase}0.{fmt}", "wb") as fh:
            fh.write(bio.getvalue())

        # Create the uncompressed version
        bio.seek(0)
        with gzip.open(bio, "rb") as fh:
            data = fh.read()
        fmt = "tif"

    for i in range(10, -1, -1):
        oldfp = f"{BASE}/{fnbase}{i}.{fmt}"
        newfp = f"{BASE}/{fnbase}{i + 1}.{fmt}"
        if os.path.isfile(oldfp):
            os.rename(oldfp, newfp)

    with open(f"{BASE}/{fnbase}0.{fmt}", "wb") as fh:
        fh.write(data)


def main(argv):
    """Do SOmething"""
    fnbase = argv[1]
    fmt = argv[2]
    dirname = f"{BASE}/{os.path.dirname(fnbase)}"
    if not os.path.isdir(dirname):
        os.makedirs(dirname)
    with BytesIO(sys.stdin.buffer.read()) as bio:
        handler(fnbase, fmt, bio)


if __name__ == "__main__":
    main(sys.argv)
