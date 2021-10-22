"""Monitor a directory for new files and then do the level II dance"""
import re
import os
import subprocess

import inotify.adapters

FNPATTERN = re.compile("OP5R_[0-9]{8}_[0-9]{4}")


def main():
    """Go Main Go."""
    i = inotify.adapters.Inotify()

    i.add_watch("/home/op5r")
    try:
        for event in i.event_gen():
            if event is not None:
                (_header, type_names, watch_path, filename) = event
                if "IN_CLOSE_WRITE" not in type_names:
                    continue
                fn = filename.decode("utf-8")
                if not FNPATTERN.match(fn):
                    print("invalid filename: %s" % (fn,))
                    continue
                cmd = f"pqinsert -f NEXRAD2 -p '{fn}' {watch_path}/{fn}"
                subprocess.call(cmd, shell=True)
                os.unlink("%s/%s" % (watch_path, fn))

    finally:
        i.remove_watch("/local/op5r")


if __name__ == "__main__":
    main()
