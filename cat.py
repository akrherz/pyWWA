#!/usr/bin/env python

import os, sys, time

r = open(sys.argv[1], 'r').read()

while (1):
  print r
  sys.stdout.flush()
  sys.stderr.write("cat.py: SENT PRODUCT!\n")
  sys.stderr.flush()
  time.sleep(10)
