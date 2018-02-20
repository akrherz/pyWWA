"""Unzip the filename given on stdin

called from pqact_iemvs.conf
"""
import sys
import os
import zipfile
import StringIO
import random
import string

BASE = "/home/ldm/data"


def ranstr():
    """used in tmpfn creation"""
    return ''.join(random.SystemRandom(
                    ).choice(string.ascii_uppercase + string.digits)
                   for _ in range(8))


def main():
    """ Read the zipfile from stdin and do important things """
    data = StringIO.StringIO(sys.stdin.read())
    zf = zipfile.ZipFile(data)
    filename = sys.argv[1]
    # Makedir if it does not exist
    dirname = "%s/%s" % (BASE, os.path.dirname(filename))
    if not os.path.isdir(dirname):
        os.makedirs(dirname)

    work = []
    for info in zf.infolist():
        fn = info.filename
        tmpfn = "%s/%s_%s" % (dirname, ranstr(), fn)
        newfn = "%s/%s" % (dirname, fn)
        output = open(tmpfn, 'wb')
        output.write(zf.read(fn))
        output.close()
        work.append([tmpfn, newfn])

    for [tmpfn, fn] in work:
        # print '%s -> %s' % (tmpfn, fn)
        os.rename(tmpfn, fn)

    # Write the file
    data.seek(0)
    output = open("%s/%s" % (BASE, filename,), 'wb')
    output.write(data.read())
    output.close()


if __name__ == '__main__':
    main()
