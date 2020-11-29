"""Convert noaaport GINI imagery into GIS PNGs

I convert raw GINI noaaport imagery into geo-referenced PNG files both in the
'native' projection and 4326.
"""
# Local
from pywwa.workflows.gini2gis import main


if __name__ == "__main__":
    main()
