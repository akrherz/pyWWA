#
# Save the zip file of current NWS WWA
# TODO: this should be removed in favour of unzip.py
#

cd /mesonet/data/gis/shape/4326/us
cat > current_ww.zip
unzip -o current_ww.zip >& /dev/null
