"""
Bulky cruft creeps into the archive noaaport IDS files, so this culls those.
"""

from datetime import timedelta
from io import BytesIO
from pathlib import Path

from pyiem.util import logger, utc

LOG = logger()

BASEDIR = Path("/mesonet/tmp/offline/text")


def main():
    """Runs for the previous UTC date."""
    dt = utc().date() - timedelta(days=1)
    for hr in range(24):
        fn = BASEDIR / f"{dt:%Y%m%d}{hr:02d}.txt"
        if not fn.exists():
            LOG.warning("Missing file %s", fn)
            continue
        newfn = fn.with_suffix(".new")
        good_bytes = 0
        culled_bytes = 0
        with open(fn, "rb") as fin, open(newfn, "wb") as fout:
            bio = BytesIO()
            for line in fin:
                if line == b"\003\001\r\r\n":
                    payload = bio.getvalue()
                    if payload.find(b"GRIB\x00") == -1:
                        fout.write(payload)
                        good_bytes += len(payload)
                    else:
                        LOG.info(repr(payload[:30]))
                        culled_bytes += len(payload)
                    bio = BytesIO()
                bio.write(line)
            fout.write(bio.getvalue())
        LOG.info("Culled %s bytes, kept %s bytes", culled_bytes, good_bytes)
        if good_bytes < 10_000_000:
            LOG.warning(
                "Processing %s resulted in %s good, %s bad, skip save",
                fn,
                good_bytes,
                culled_bytes,
            )
            continue
        fn.unlink()
        newfn.rename(fn)


if __name__ == "__main__":
    main()
