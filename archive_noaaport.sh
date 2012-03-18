#
# Cron script to tar up saved raw text data
# ldm's cronscript is not on svn yet
dd=`date --date "$1 day ago" +'%d'`
yyyymmdd=`date --date "$1 day ago" +'%Y%m%d'`
yyyy=`date --date "$1 day ago" +'%Y'`

cd ~/offline/text/
tar -czf ${yyyymmdd}.tgz ${yyyymmdd}??.txt
rm -f ${yyyymmdd}??.txt
mkdir -p /mesonet/ARCHIVE/raw/noaaport/$yyyy
mv ${yyyymmdd}.tgz /mesonet/ARCHIVE/raw/noaaport/$yyyy/

