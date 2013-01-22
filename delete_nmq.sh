#!/bin/bash
# We don't have room for all the Q2 data, but we need some of it! :)

export DIR="/mnt/mtarchive/data/`date --date '1 day ago' -u +'%Y/%m/%d'`/nmq/"

rm -rf ${DIR}tile1
rm -rf ${DIR}tile[3-8]
rm -rf ${DIR}tile2/data/QPESUMS/grid/mosaic*
