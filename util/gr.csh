#!/bin/csh -f
set nexrad=$1

cd /mnt/level2/raw/$1

# ls --block-size=1 -1as > dir2.list
ls -l ${1}* | grep -v _MDM.arv2 | awk '{print $5 " " $9}' > dir2.list
cp dir2.list dir.list
rm -f dir2.list
