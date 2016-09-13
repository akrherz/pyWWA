"""Monitor a directory for new files and then do the level II dance"""
import inotify.adapters
import re
import os
import shutil
import subprocess

FNPATTERN = re.compile("OP5R_[0-9]{8}_[0-9]{4}")


def main():
    i = inotify.adapters.Inotify()

    i.add_watch('/local/op5r')
    try:
        for event in i.event_gen():
            if event is not None:
                (header, type_names, watch_path, filename) = event
                if 'IN_CLOSE_WRITE' not in type_names:
                    continue
                fn = filename.decode('utf-8')
                if not FNPATTERN.match(fn):
                    print("invalid filename: %s" % (fn,))
                    continue
                shutil.move("%s/%s" % (watch_path, fn),
                            "/mnt/level2/raw/OP5R/%s" % (fn,))
                os.chdir("/mnt/level2/raw/OP5R")
                subprocess.call(("ls -l OP5R* | "
                                 "awk '{print $5 " " $9}' > dir2.list"
                                 ), shell=True)
                shutil.move("dir2.list", "dir.list")

    finally:
        i.remove_watch('/local/op5r')


if __name__ == '__main__':
    main()
