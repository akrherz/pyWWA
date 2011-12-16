#
# Cron script to tar up saved raw text data
# ldm's cronscript is not on svn yet
set dd=`date --date "$1 day ago" +'%d'`
set yyyymmdd=`date --date "$1 day ago" +'%Y%m%d'`
set yyyy=`date --date "$1 day ago" +'%Y'`

cd ~/offline/text/
tar -czf ${yyyymmdd}.tgz ${yyyymmdd}??.txt
rm -f ${yyyymmdd}??.txt
mkdir -p /mesonet/ARCHIVE/raw/noaaport/$yyyy
mv ${yyyymmdd}.tgz /mesonet/ARCHIVE/raw/noaaport/$yyyy/

