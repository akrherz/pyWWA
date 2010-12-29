set radfile=rad.$$

cd /tmp
cat > $radfile

source /mesonet/nawips/Gemenviron

/home/ldm/bin/gpnids_vg << EOF > /tmp/log.$$
 RADFIL   = /tmp/$radfile
 RADTIM   =
 TITLE    = 1
 PANEL    = 0
 DEVICE   = GIF|$$.gif
 CLEAR    = YES
 TEXT     = 1
 COLORS   = 1
 WIND     = 
 LINE     = 3
 CLRBAR   =
 IMCBAR   =
 GAREA    = DSET
 MAP      = 1
 LATLON   =
 OUTPUT   = f/radar.$$
 run

 exit
EOF

#cat /tmp/log.$$
#cat /tmp/radar.$$
/mesonet/python/bin/python /home/ldm/pyWWA/ncr2postgis.py /tmp/radar.$$ $1 $2 $3 $4 $5 $6

rm -f $radfile log.$$ rad.$$ radar.$$ $$.gif

exit 0
