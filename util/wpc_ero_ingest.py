"""Fake a NOAAPort product for WPC ERO."""
import tempfile
import subprocess
import os

import requests
from pyiem.nws.product import TextProduct
from pyiem.util import (
    logger,
    get_dbconn,
    utc,
    exponential_backoff,
    noaaport_text,
)

LOG = logger()
BASEURL = "https://www.wpc.ncep.noaa.gov/qpf/"
QUEUE = {
    "94epoints.txt": ["MENC98", "RBG94E"],
    "94epoints_remaining.txt": ["MENC98", "RBG94E"],
    "98epoints.txt": ["MENU98", "RBG98E"],
    "99epoints.txt": ["MENU98", "RBG99E"],
}


def run(cursor, fn, ttaaii, awipsid):
    """Make things happen."""
    cursor.execute(
        "select entered from products where pil = %s and "
        "entered > now() - '2 days'::interval and entered < now() "
        "ORDER by entered DESC",
        (awipsid,),
    )
    current = utc(1980)
    if cursor.rowcount > 0:
        current = cursor.fetchone()[0]
    req = exponential_backoff(requests.get, f"{BASEURL}/{fn}", timeout=30)
    if req is None or req.status_code != 200:
        LOG.info("failed to fetch %s", fn)
        return
    data = "\n".join(["000 ", f"{ttaaii} KWNH 010000", awipsid, ""]) + req.text
    tp = TextProduct(data, parse_segments=False)
    LOG.debug("For %s current: %s, tp.valid: %s", awipsid, current, tp.valid)
    if tp.valid <= current:
        return
    ts = tp.valid.strftime("%d%H%M")
    data = data.replace("KWNH 010000\n", f"KWNH {ts}\n")
    data = noaaport_text(data)

    tmpfd = tempfile.NamedTemporaryFile(delete=False)
    with open(tmpfd.name, "wb") as fh:
        fh.write(data.encode("ascii"))
    cmd = f"pqinsert -f IDS -p '{ttaaii} KWNH {ts} /p{awipsid}' {tmpfd.name}"
    LOG.debug(cmd)
    subprocess.call(cmd, shell=True)
    os.unlink(tmpfd.name)


def main():
    """Go Main Go."""
    with get_dbconn("afos") as pgconn:
        cursor = pgconn.cursor()
        for fn, (ttaaii, awipsid) in QUEUE.items():
            run(cursor, fn, ttaaii, awipsid)


if __name__ == "__main__":
    main()
