gzip -d > /tmp/$$.$2
set F=`echo $2 | cut -c1-13`
mv /tmp/$$.$2 $1/$F.nc