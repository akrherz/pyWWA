#!/bin/sh
# Store the MADIS netCDF files to where they should go $1

gzip -d > /tmp/$$.$2
F=$(echo $2 | cut -c1-13)
mv /tmp/$$.$2 $1/$F.nc
