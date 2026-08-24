#!/bin/bash
# This is called from LDM pqact entries in akrherz/ldmconfig repo
set -eo pipefail

nexrad="$1"

cd "/mnt/level2/raw/$nexrad"

# shellcheck disable=SC2010
ls -ln "${nexrad}"* | grep -v _MDM.arv2 | awk '{print $5 " " $9}' > dir2.list
cp dir2.list dir.list
rm -f dir2.list
