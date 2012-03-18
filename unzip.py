"""
Do the rotation action that some products need
$Id: $:
"""
import sys
import os
import gzip

if __name__ == '__main__':
    # blah/file_
    data = sys.stdin.read()
    filename = sys.argv[1]
    dirname = "/home/ldm/data/" +  "/".join( filename.split("/")[:-1] )
    if not os.path.isdir(dirname):
        os.makedirs(dirname)

    o = open("/home/ldm/data/%s" % (filename,), 'wb')
    o.write(data)
    o.close()
    
    os.chdir(dirname)
    os.system("unzip -o /home/ldm/data/%s" % (filename,))