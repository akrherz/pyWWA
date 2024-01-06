"""METAR product ingestor

NOTE: It is difficult to keep track of where I am getting the `Metar` library.
So let us document it here for my own sanity.

18 Jul 2017: `snowdepth` branch of my python-metar fork installed with pip
"""
# Local
from pywwa.workflows.metar import main

if __name__ == "__main__":
    main()
