"""An infinite loop to get ridge_runner.py going."""

import os
import subprocess
import time


def main():
    """Infinite loop here"""
    scriptfn = os.sep.join([os.path.dirname(__file__), "ridge_processor.py"])
    while True:
        print("ridge_runner.py is execing ridge_processor.py now...")
        subprocess.call(["python", scriptfn])
        time.sleep(3)


if __name__ == "__main__":
    main()
