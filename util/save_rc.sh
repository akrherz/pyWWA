#
# This is run async after the ingestor fires a product into LDM
# which triggers this script

ts="$1"
hhmm=$(echo $1 | cut -c 9-12)

wget -q -O /tmp/iaroads.png 'http://iem.local/roads/iem.php?8bit'

pqinsert -p "plot ac $ts iaroads.png iaroads_${hhmm}.png png" /tmp/iaroads.png

rm -f /tmp/iaroads.png
