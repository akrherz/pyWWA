#!/bin/sh
# Something to compress the MADIS data, run from crontab_iem21.txt

YYYYMMDD=$(date --date '1 day ago' +'%Y%m%d')
YYYY=$(date --date '1 day ago' +'%Y')

dirs=("metar" "mesonet1" "hfmetar")
for p in "${dirs[@]}"
do
cd /mesonet/data/madis/$p
gzip ${YYYYMMDD}*nc
mkdir -p /mnt/longterm2/madis/$p/${YYYY}
mv ${YYYYMMDD}*nc.gz /mnt/longterm2/madis/$p/${YYYY}
done

#END
