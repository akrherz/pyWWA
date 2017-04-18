#!/bin/sh
# Store the MADIS netCDF files to where they should go $1

gzip -d > /tmp/$$.$2
F=$(echo $2 | cut -c1-13)
# Had an issue with random failures on some days, so remove files if fail
mv /tmp/$$.$2 $1/$F.nc || rm -f /tmp/$$.$2

#End
