"""Unzip the filename given on stdin

called from pqact_iemvs.conf
"""

import os
import random
import string
import sys
import zipfile
from io import BytesIO

BASE = "/mesonet/ldmdata"


def ranstr():
    """used in tmpfn creation"""
    return "".join(
        random.SystemRandom().choice(string.ascii_uppercase + string.digits)
        for _ in range(8)
    )


def main(argv):
    """Read the zipfile from stdin and do important things"""
    data = BytesIO(sys.stdin.buffer.read())
    zf = zipfile.ZipFile(data)
    filename = argv[1]
    # Makedir if it does not exist
    dirname = f"{BASE}/{os.path.dirname(filename)}"
    if not os.path.isdir(dirname):
        os.makedirs(dirname)

    work = []
    for info in zf.infolist():
        fn = info.filename
        tmpfn = f"{dirname}/{ranstr()}_{fn}"
        newfn = f"{dirname}/{fn}"
        with open(tmpfn, "wb") as fp:
            fp.write(zf.read(fn))
        work.append([tmpfn, newfn])

    for [tmpfn, fn] in work:
        os.rename(tmpfn, fn)

    # Write the file
    data.seek(0)
    with open(f"{BASE}/{filename}", "wb") as fp:
        fp.write(data.read())


if __name__ == "__main__":
    main(sys.argv)
