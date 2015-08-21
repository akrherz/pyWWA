#!/bin/sh
# Something to compress the MADIS data, run from crontab_iem21.txt

YYYYMMDD=$(date --date '1 day ago' +'%Y%m%d')
YYYY=$(date --date '1 day ago' +'%Y')

cd /mesonet/data/madis/metar
gzip ${YYYYMMDD}*nc
mkdir -p /mnt/longterm2/madis/metar/${YYYY}
mv ${YYYYMMDD}*nc.gz /mnt/longterm2/madis/metar/${YYYY}

cd /mesonet/data/madis/mesonet
gzip ${YYYYMMDD}*nc
mkdir -p /mnt/longterm2/madis/mesonet/${YYYY}
mv ${YYYYMMDD}*nc.gz /mnt/longterm2/madis/mesonet/${YYYY}

#END

