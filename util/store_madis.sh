#!/bin/bash
# Store the MADIS netCDF files to where they should go $1

gzip -d > /tmp/$$.$2
F=$(echo $2 | cut -c1-13)

# We have some time intensive readers, so we keep copies around
for i in $(seq 0 300); do
    if [ -f $1/${F}_${i}.nc ]; then
        continue;
    fi
    # Had an issue with random failures on some days, so remove files if fail
    mv /tmp/$$.$2 $1/${F}_${i}.nc || rm -f /tmp/$$.$2
    break;
done


# End
