# This is run async after the ingestor fires a product into LDM
# which triggers this script
# Run from: pqact_filer.csh
set ts="$1"
set hhmm="`echo $1 | cut -c 9-12`"

wget -q -O /tmp/iaroads.png 'http://iemvs101.local/roads/iem.php?8bit'

pqinsert -p "plot ac $ts iaroads.png iaroads_${hhmm}.png png" /tmp/iaroads.png
