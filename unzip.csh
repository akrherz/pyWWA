#!/bin/csh

# Argument is the filename base!
set fn="$1"
set mydir="/home/ldm/data/${fn:h}"
set myfile="${fn:t}"
mkdir -p $mydir

cat > $mydir/$myfile
echo $mydir
cd $mydir
unzip -o $myfile
