"""Some magic to feed the SEL, SAW, and WWP products into the LDM bridge."""

import sys
import time


def main(argv):
    """Go Main Go."""
    with open(argv[1], "rb") as fh:
        payload = fh.read()
    while True:
        sys.stdout.buffer.write(payload)
        time.sleep(1)


if __name__ == "__main__":
    main(sys.argv)
