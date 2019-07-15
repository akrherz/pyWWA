"""Make a file on disk appear more noaaport-ish."""
import sys

from pyiem.util import noaaport_text


def main(argv):
    """Do Main Things"""
    fn = argv[1]
    data = noaaport_text(open(fn).read())
    output = open(fn, 'w')
    output.write(data)
    output.close()


if __name__ == '__main__':
    main(sys.argv)
