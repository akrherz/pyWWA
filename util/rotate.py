"""
Do the rotation action that some products need
"""
import sys
import os
import gzip

BASE = "/home/ldm/data/"

if __name__ == '__main__':
    # blah/file_
    data = sys.stdin.read()
    fnbase = sys.argv[1]
    fmt = sys.argv[2]

    dirname = "%s/%s" % (BASE, os.path.dirname(fnbase))
    if not os.path.isdir(dirname):
        os.makedirs(dirname)

    if fmt == "tif.Z":
        for i in range(9, -1, -1):
            oldfp = "%s/%s%s.%s" % (BASE, fnbase, i, fmt)
            newfp = "%s/%s%s.%s" % (BASE, fnbase, i+1, fmt)
            if os.path.isfile(oldfp):
                os.rename(oldfp, newfp)

        o = open("%s/%s%s.%s" % (BASE, fnbase, 0, fmt), 'wb')
        o.write(data)
        o.close()

        data = gzip.open("%s/%s%s.%s" % (BASE, fnbase, 0, fmt), 'rb').read()
        fmt = "tif"

    for i in range(9, -1, -1):
        oldfp = "%s/%s%s.%s" % (BASE, fnbase, i, fmt)
        newfp = "%s/%s%s.%s" % (BASE, fnbase, i+1, fmt)
        if os.path.isfile(oldfp):
            os.rename(oldfp, newfp)

    o = open("%s/%s%s.%s" % (BASE, fnbase, 0, fmt), 'wb')
    o.write(data)
    o.close()
