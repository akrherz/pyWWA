"""An infinite loop to get ridge_runner.py going."""
import time
import subprocess


def main():
    """Infinite loop here"""
    while True:
        print("ridge_runner.py is execing ridge_processor.py now...")
        subprocess.call(["python", "ridge_processor.py"])
        time.sleep(3)


if __name__ == "__main__":
    main()
