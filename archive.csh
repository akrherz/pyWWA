#
# Cron script to tar up saved raw text data
# ldm's cronscript is not on svn yet
set dd=`date --date "$1 day ago" +'%d'`
set yyyymmdd=`date --date "$1 day ago" +'%Y%m%d'`

cd ~/offline/text/
tar -czf ${yyyymmdd}.tgz ${dd}??.txt
rm -f ${dd}??.txt
mv ${yyyymmdd}.tgz ~/archive/