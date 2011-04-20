set BASE="$1"
set FMT="$2"
set SSS="/home/ldm/data/${BASE}"
mkdir -p ${SSS:h} >& /dev/null


if ($FMT == "tif.Z") then
  # Copy around the Z file
  cat > /tmp/file.$$.Z

  foreach i (9 8 7 6 5 4 3 2 1 0)
    set j = `echo "${i} + 1" | bc `
    mv /home/ldm/data/${BASE}${i}.${FMT} /home/ldm/data/${BASE}${j}.${FMT}
  end

  cp /tmp/file.$$.Z /home/ldm/data/${BASE}0.${FMT}

  gunzip /tmp/file.$$.Z
  set FMT="tif"
else 
  cat > /tmp/file.$$

endif

  foreach i (9 8 7 6 5 4 3 2 1 0)
    set j = `echo "${i} + 1" | bc `
    mv /home/ldm/data/${BASE}${i}.${FMT} /home/ldm/data/${BASE}${j}.${FMT}
  end

  mv /tmp/file.$$ /home/ldm/data/${BASE}0.${FMT}
  