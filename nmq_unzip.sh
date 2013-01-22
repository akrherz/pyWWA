#!/bin/bash

cd /tmp
cat > $$.gz
mkdir -p /mnt/mtarchive/data/$2/$3/$4/nmq/$1
gunzip -c $$.gz > /mnt/mtarchive/data/$2/$3/$4/nmq/$1/$2$3$4-$5.nc
rm $$.gz
