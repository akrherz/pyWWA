# Archive NWWS-OI data from 7 days ago

yyyymmdd=$(date --date "7 day ago" +'%Y%m%d')
yyyy=$(date --date "7 day ago" +'%Y')

cd /mesonet/tmp/nwwsoi/
tar -czf ${yyyymmdd}.tgz ${yyyymmdd}??.txt
rm -f ${yyyymmdd}??.txt

rpath="/offline/NWWSOI/${yyyy}"
rsync -a --remove-source-files --rsync-path "mkdir -p $rpath && rsync" ${yyyymmdd}.tgz meteor_ldm@akrherz-desktop.agron.iastate.edu:$rpath

# END
