#!/bin/sh
# Simple Script that will save RADAR data
# Run from cron
#set -x

DIR=$(date -u -d "1 day ago" +'%Y_%m')
YYYYMMDD=$(date -u -d "1 day ago" +'%Y%m%d')

if [ ! -d /mesonet/ARCHIVE/nexrad/${DIR} ] 
then
	mkdir -p /mesonet/ARCHIVE/nexrad/${DIR}
fi

for R in $(echo "DMX DVN OAX ARX FSD MPX EAX ABR UDX"); do
cd /mnt/nexrad3/nexrad/NIDS/${R}
  tar -cf /tmp/${R}radar.tar ?[A-Z]?/???_${YYYYMMDD}_*
  tar -uf /tmp/${R}radar.tar ?[0-9]?/???_${YYYYMMDD}_*
  cd /tmp
  gzip -c ${R}radar.tar > ${R}radar.tgz
  mv ${R}radar.tgz /mesonet/ARCHIVE/nexrad/${DIR}/${R}_${YYYYMMDD}.tgz
  rm ${R}radar.tar
done

#END

