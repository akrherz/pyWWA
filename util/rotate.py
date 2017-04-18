"""
Do the rotation action that some products need
"""
import sys
import os
import gzip

BASE = "/home/ldm/data/"


def main(argv):
    """Do SOmething"""
    data = sys.stdin.read()
    fnbase = argv[1]
    fmt = argv[2]

    dirname = "%s/%s" % (BASE, os.path.dirname(fnbase))
    if not os.path.isdir(dirname):
        os.makedirs(dirname)

    if fmt == "tif.Z":
        for i in range(9, -1, -1):
            oldfp = "%s/%s%s.%s" % (BASE, fnbase, i, fmt)
            newfp = "%s/%s%s.%s" % (BASE, fnbase, i+1, fmt)
            if os.path.isfile(oldfp):
                os.rename(oldfp, newfp)

        output = open("%s/%s%s.%s" % (BASE, fnbase, 0, fmt), 'wb')
        output.write(data)
        output.close()

        data = gzip.open("%s/%s%s.%s" % (BASE, fnbase, 0, fmt), 'rb').read()
        fmt = "tif"

    for i in range(9, -1, -1):
        oldfp = "%s/%s%s.%s" % (BASE, fnbase, i, fmt)
        newfp = "%s/%s%s.%s" % (BASE, fnbase, i+1, fmt)
        if os.path.isfile(oldfp):
            os.rename(oldfp, newfp)

    output = open("%s/%s%s.%s" % (BASE, fnbase, 0, fmt), 'wb')
    output.write(data)
    output.close()


if __name__ == '__main__':
    main(sys.argv)
