"""
 need something that keeps ridge_processor alive and kicking
"""
from __future__ import print_function
import time
import subprocess


def main():
    """Infinite loop here"""
    while True:
        print("ridge_runner.py is execing ridge_processor.py now...")
        subprocess.call("python ridge_processor.py", shell=True)
        time.sleep(3)


if __name__ == '__main__':
    main()
