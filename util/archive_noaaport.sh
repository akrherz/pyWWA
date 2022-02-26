# Cron script to tar up saved raw text data and place on the website for 
# others to download here
# https://mesonet.agron.iastate.edu/archive/raw/noaaport/

dd=$(date --date "$1 day ago" +'%d')
yyyymmdd=$(date --date "$1 day ago" +'%Y%m%d')
yyyy=$(date --date "$1 day ago" +'%Y')
mm=$(date --date "$1 day ago" +'%m')

cd /mesonet/tmp/offline/text/
tar -czf ${yyyymmdd}.tgz ${yyyymmdd}??.txt
rm -f ${yyyymmdd}??.txt
mkdir -p /mesonet/ARCHIVE/raw/noaaport/$yyyy

rpath="/stage/NOAAPortText/${yyyy}/${mm}"
rsync -a --rsync-path "mkdir -p $rpath && rsync" ${yyyymmdd}.tgz meteor_ldm@metl60.agron.iastate.edu:$rpath

mv ${yyyymmdd}.tgz /mesonet/ARCHIVE/raw/noaaport/$yyyy/
# END
