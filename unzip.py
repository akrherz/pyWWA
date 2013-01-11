"""
 Unzip the filename given on stdin
"""
import sys
import os

BASE = "/home/ldm/data"

def main():
    # blah/file_
    data = sys.stdin.read()
    filename = sys.argv[1]
    # Makedir if it does not exist
    dirname = "%s/%s" % (BASE, os.path.dirname(filename) )
    if not os.path.isdir(dirname):
        os.makedirs(dirname)
    
    # Write the file
    o = open("%s/%s" % (BASE, filename,), 'wb')
    o.write(data)
    o.close()
    
    os.system("unzip -d %s -o %s/%s" % (dirname, BASE, filename))
    
if __name__ == '__main__':
    main()