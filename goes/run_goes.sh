# Crude restarting script

while true; do
  python netcdf2png.py $1
  echo "netcdf2png.py $1 restarted" | mailx -s 'GOES Restarted' akrherz@iastate.edu
  sleep 60
done
