#!/bin/sh
# Save Iowa Level III data to cybox 
# Run from cron
#set -x

DIR=$(date -u -d "1 day ago" +'%Y_%m')
YYYYMMDD=$(date -u -d "1 day ago" +'%Y%m%d')
YYYY=$(date -u -d "1 day ago" +'%Y')
MM=$(date -u -d "1 day ago" +'%m')

for R in $(echo "DMX DVN OAX ARX FSD MPX EAX ABR UDX"); do
cd /mnt/nexrad3/nexrad/NIDS/${R}
  tar -cf /tmp/${R}radar.tar ?[A-Z]?/???_${YYYYMMDD}_*
  tar -uf /tmp/${R}radar.tar ?[0-9]?/???_${YYYYMMDD}_*
  cd /tmp
  gzip -c ${R}radar.tar > ${R}radar.tgz
mv ${R}radar.tgz ${R}_${YYYYMMDD}.tgz
lftp -u akrherz@iastate.edu ftps://ftp.box.com << EOM
cd IowaNexrad3
mkdir ${YYYY}
cd ${YYYY}
mkdir ${MM}
cd ${MM}
put ${R}_${YYYYMMDD}.tgz
bye
EOM
rm ${R}radar.tar ${R}_${YYYYMMDD}.tgz
done

#END

