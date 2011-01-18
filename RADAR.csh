# Generate a geo-referenced image for DMX
# Daryl Herzmann 17 Jun 2003

source /mesonet/nawips/Gemenviron

# Wait for data to settle
sleep 3

cd ~/pyWWA

set SITE=$1
set PROD=$2
set YYYY=$3
set MO=$4
set DD=$5
set HH=$6
set MM=$7

set dbfts = `date -s "${YYYY}/${MO}/${DD} ${HH}:${MM}" +'%d %b %Y %H:%M GMT'`

dbfcreate /mesonet/data/gis/images/26915/${SITE}/meta/${SITE}_${PROD}_0.dbf -s ts 100
dbfadd /mesonet/data/gis/images/26915/${SITE}/meta/${SITE}_${PROD}_0.dbf "${SITE} ${PROD} ${dbfts}"

setenv DISPLAY localhost:1

rm -f ${SITE}_${PROD}_out.gif >& /dev/null

gpmap_gf << EOF > DMX_gpmap.log
 MAP = 0
 GAREA = DSET
 PROJ  = RAD//nm
 SATFIL = 
 RADFIL = /mesonet/data/nexrad/NIDS/${SITE}/${PROD}/${PROD}_${YYYY}${MO}${DD}_${HH}${MM}
 LATLON = 0
 PANEL = 0
 TITLE = 0
 TEXT  = 1/1/sw
 CLEAR = YES
 DEVICE = GF|${SITE}_${PROD}_out.gif|500;500
 LUTFIL = 
 STNPLT   =  
 VGFILE   =  
 AFOSFL   =  
 AWPSFL   =  
 LINE     = 1///0
 WATCH    =  
 WARN     =  
 HRCN     =  
 ISIG     =  
 LTNG     =  
 ATCF     =  
 AIRM     =  
 NCON     =  
 SVRL     =  
 BND      =  
 ATCO     =  
 TCMG     =  
 QSCT     =  
 IMBAR    = 0
 list
 run

exit
EOF

if (-e ${SITE}_${PROD}_out.gif) then
  convert ${SITE}_${PROD}_out.gif ${SITE}_${PROD}_out.png
  convert ${SITE}_${PROD}_out.gif ${SITE}_${PROD}_out.tif
  mv ${SITE}_${PROD}_out.png /mesonet/data/gis/images/26915/${SITE}/${SITE}_${PROD}_0.png
  mv ${SITE}_${PROD}_out.tif /mesonet/data/gis/images/26915/${SITE}/${SITE}_${PROD}_0.tif
endif
