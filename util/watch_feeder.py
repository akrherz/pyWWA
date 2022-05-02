"""Some magic to feed the SEL, SAW, and WWP products into the LDM bridge."""
import time
import sys


def main():
    """Go Main Go."""
    for prod in ['SEL', 'SAW', 'WWP']:
        sys.stderr.write(f"Writing {prod}\n")
        with open(f"examples/{prod}.txt", "rb") as fh:
            sys.stdout.buffer.write(fh.read())
            sys.stdout.flush()
            time.sleep(10)
    sys.stderr.write("Sleeping 120\n")
    time.sleep(120)


if __name__ == "__main__":
    main()
