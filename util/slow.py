import sys
import time
from pyiem.util import noaaport_text

for i, prod in enumerate(open(sys.argv[1]).read().split("\003")):
    prod = noaaport_text(prod)
    #if prod.find("MDT ") == -1:
    #  continue
    sys.stdout.write(prod)
    sys.stdout.flush()
    time.sleep(0.1)
