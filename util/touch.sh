# frontend to touch that also makes needed directories
# $1 is the file to touch
mkdir -p $(dirname $1)
touch $1
