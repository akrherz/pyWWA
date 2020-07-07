#!/bin/sh
# Something to compress the MADIS data, run from crontab_iem21.txt

YYYYMMDD=$(date --date '1 day ago' +'%Y%m%d')
YYYY=$(date --date '1 day ago' +'%Y')

dirs=("metar" "mesonet1" "hfmetar")
for p in "${dirs[@]}"
do
  cd /mesonet/data/madis/$p
  gzip ${YYYYMMDD}*nc
  rsync -a --remove-source-files \
    --rsync-path "mkdir -p /stage/iemoffline/madis/$p/$YYYY && rsync" \
    ${YYYYMMDD}*nc.gz metl60.agron.iastate.edu:/stage/iemoffline/madis/$p/$YYYY/
done

#END
