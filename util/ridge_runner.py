"""
 need something that keeps ridge_processor alive and kicking
"""
import time
import subprocess
while True:
    subprocess.call("python ridge_processor.py", shell=True)
    time.sleep(3)
