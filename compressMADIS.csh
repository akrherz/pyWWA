#!/bin/csh
# Something to compress the MADIS data

set YYYYMMDD="`date --date '1 day ago' +'%Y%m%d'`"

cd /mesonet/data/madis/metar
gzip ${YYYYMMDD}*nc
cd /mesonet/data/madis/mesonet
gzip ${YYYYMMDD}*nc
