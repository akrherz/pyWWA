# Process NCR data!
#

set radfile=rad.$$

cd /tmp
cat > $radfile

set LOCK=.inuse.$$
touch $LOCK

@ COUNT = 0
set TEST=`ls -rt .inuse.* | head -1`
set OFFENDING=$TEST
while(($TEST != $LOCK)&&($COUNT < 361))
   sleep 1
   set TEST=`ls -rt .inuse.* | head -1`
   if ( ( $COUNT == 30 ) && ( $TEST == $OFFENDING ) ) then
      # this lock has been around a really long time. Maybe its toast.
      rm -f $OFFENDING
   endif
   if($COUNT == 360) then
      rm $LOCK
      exit 0
   endif
   @ COUNT = $COUNT + 1
end
# go go go
rm $LOCK

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
python /home/ldm/pyWWA/ncr2postgis.py /tmp/radar.$$ $1 $2 $3 $4 $5 $6

rm -f $radfile log.$$ rad.$$ radar.$$ $$.gif

exit 0

