"""
 Unzip the filename given on stdin

called from pqact_iemvs.conf
"""
import sys
import os
import subprocess

BASE = "/home/ldm/data"

def main():
    """ Do something important """
    data = sys.stdin.read()
    filename = sys.argv[1]
    # Makedir if it does not exist
    dirname = "%s/%s" % (BASE, os.path.dirname(filename))
    if not os.path.isdir(dirname):
        os.makedirs(dirname)

    # Write the file
    output = open("%s/%s" % (BASE, filename,), 'wb')
    output.write(data)
    output.close()

    proc = subprocess.Popen("unzip -d %s -o %s/%s" % (dirname, BASE, filename),
                            shell=True, stderr=subprocess.PIPE,
                            stdout=subprocess.PIPE)
    res = proc.stderr.read()
    if res != "":
        print res
        sys.exit(1)

if __name__ == '__main__':
    main()
