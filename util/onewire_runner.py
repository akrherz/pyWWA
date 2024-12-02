"""Poll the OneWire bus for devices."""

import os
import subprocess
import time
from datetime import datetime

os.environ["TZ"] = "CST6CDT"


def work():
    """Do the work."""
    with subprocess.Popen(
        ["./digitemp", "-q", "-a", "-s", "/dev/ttyS0", "-o", "%s %.2F"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    ) as proc:
        stdout, stderr = proc.communicate()
    if stderr != b"":
        print("Rut roh")
        print(stderr.decode("ascii"))
        return
    d = stdout.decode("ascii").split("\n")
    with open("onewire.txt", "w") as fh:
        fh.write("\n".join(d))
    data = {}
    for line in d:
        if not line:
            continue
        t = line.split()
        try:
            data[int(t[0])] = t[1]
        except Exception:
            print(line)
    if len(data) < 4:
        print("ERROR: not enough data")
        return
    now = datetime.now()
    fp = f"ot0003_{now:%Y%m%d%H%M}.dat"
    with open(fp, "w") as fh:
        fh.write(
            f"104,{now:%Y},{now:%j},{now:%H%M},{data[0]}, "
            f"{data[1]}, {data[2]}, {data[3]},11.34\n"
        )
    subprocess.call(["/home/meteor_ldm/bin/pqinsert", fp])
    os.remove(fp)


def main():
    """Go Main Go."""
    with open("runner.pid", "w") as fh:
        fh.write(f"{os.getpid()}")
    while 1:
        work()
        time.sleep(58)


if __name__ == "__main__":
    main()
