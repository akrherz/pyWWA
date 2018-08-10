# Cron script to tar up saved raw text data and place on the website for 
# others to download here
# http://mesonet.agron.iastate.edu/archive/raw/noaaport/

dd=$(date --date "$1 day ago" +'%d')
yyyymmdd=$(date --date "$1 day ago" +'%Y%m%d')
yyyy=$(date --date "$1 day ago" +'%Y')
mm=$(date --date "$1 day ago" +'%m')

cd ~/offline/text/
tar -czf ${yyyymmdd}.tgz ${yyyymmdd}??.txt
rm -f ${yyyymmdd}??.txt
mkdir -p /mesonet/ARCHIVE/raw/noaaport/$yyyy

# Upload this file to box
lftp -u akrherz@iastate.edu ftps://ftp.box.com << EOM
cd NOAAPortText
mkdir ${yyyy}
cd ${yyyy}
mkdir ${mm}
cd ${mm}
put ${yyyymmdd}.tgz
bye
EOM


mv ${yyyymmdd}.tgz /mesonet/ARCHIVE/raw/noaaport/$yyyy/
# END
