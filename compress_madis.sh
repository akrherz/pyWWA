#!/bin/sh
# Something to compress the MADIS data, run from crontab_iem21.txt

YYYYMMDD=$(date --date '1 day ago' +'%Y%m%d')
YYYY=$(date --date '1 day ago' +'%Y')

cd /mesonet/data/madis/metar
gzip ${YYYYMMDD}*nc
mkdir -p /mnt/longterm2/metar/${YYYY}
mv ${YYYYMMDD}*nc.gz /mnt/longterm2/metar/${YYYY}

cd /mesonet/data/madis/mesonet
gzip ${YYYYMMDD}*nc
mkdir -p /mnt/longterm2/mesonet/${YYYY}
mv ${YYYYMMDD}*nc.gz /mnt/longterm2/mesonet/${YYYY}

#END