#!/bin/csh
# Simple Script that will save RADAR data
# Run from cron

set DIR = `date -u -d "1 day ago" +%Y_%m`
set YYYYMMDD = `date -u -d "1 day ago" +%Y%m%d`

if (! -e /mesonet/ARCHIVE/nexrad/${DIR} ) then
	mkdir /mesonet/ARCHIVE/nexrad/${DIR}
endif

foreach R (DMX DVN OAX ARX FSD MPX EAX ABR UDX)
  cd /mesonet/data/nexrad/NIDS/${R}
  tar -cf /tmp/${R}radar.tar ?[A-Z]?/???_${YYYYMMDD}_*
  tar -uf /tmp/${R}radar.tar ?[0-9]?/???_${YYYYMMDD}_*
  cd /tmp
  gzip -c ${R}radar.tar > ${R}radar.tgz
  mv ${R}radar.tgz /mesonet/ARCHIVE/nexrad/${DIR}/${R}_${YYYYMMDD}.tgz
  rm ${R}radar.tar
end
