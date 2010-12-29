set dd=`date --date "$1 day ago" +'%d'`
set yyyymmdd=`date --date "$1 day ago" +'%Y%m%d'`

cd ~/offline/text/
tar -czf ${yyyymmdd}.tgz ${dd}??.txt
rm -f ${dd}??.txt
mv ${yyyymmdd}.tgz ~/archive/