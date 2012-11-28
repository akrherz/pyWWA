"""
Do the rotation action that some products need
"""
import sys
import os
import gzip

if __name__ == '__main__':
    # blah/file_
    data = sys.stdin.read()
    fnbase = sys.argv[1]
    fmt = sys.argv[2]

    dirname = "/home/ldm/data/" +  "/".join( fnbase.split("/")[:-1] )
    if not os.path.isdir(dirname):
        os.makedirs(dirname)

    if fmt == "tif.Z":
        for i in range(9,-1,-1):
            oldfp = "/home/ldm/data/%s%s.%s" % (fnbase,i,fmt)
            newfp = "/home/ldm/data/%s%s.%s" % (fnbase,i+1,fmt)
            if os.path.isfile(oldfp):
                os.rename(oldfp, newfp)

        o = open("/home/ldm/data/%s%s.%s" % (fnbase,0,fmt), 'wb')
        o.write(data)
        o.close()
        
        data = gzip.open("/home/ldm/data/%s%s.%s" % (fnbase,0,fmt), 'rb').read()
        fmt = "tif"

    for i in range(9,-1,-1):
        oldfp = "/home/ldm/data/%s%s.%s" % (fnbase,i,fmt)
        newfp = "/home/ldm/data/%s%s.%s" % (fnbase,i+1,fmt)
        if os.path.isfile(oldfp):
            os.rename(oldfp, newfp)

    o = open("/home/ldm/data/%s%s.%s" % (fnbase,0,fmt), 'wb')
    o.write(data)
    o.close()