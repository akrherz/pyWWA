# Run the various examples through their ingest
set -x
OPTS="-l -x -s 1 -e"

cat examples/ESF.txt | python parsers/afos_dump.py $OPTS || exit 2

cat examples/SIGC.txt | python parsers/aviation.py $OPTS || exit 2

cat examples/CLIANN.txt | python parsers/cli_parser.py $OPTS || exit 2

cat examples/DSM.txt | python parsers/dsm2afos.py DSM || exit 2

cat examples/DSM.txt | python parsers/dsm_parser.py $OPTS || exit 2

cat examples/CWA.txt | python parsers/fake_afos_dump.py $OPTS || exit 2

cat examples/FFGDMX.txt | python parsers/ffg_parser.py $OPTS || exit 2

cat examples/AFD.txt | python parsers/generic_parser.py $OPTS || exit 2

cat examples/HMLARX.txt | python parsers/hml_parser.py $OPTS || exit 2

cat examples/LSR.txt | python parsers/lsr_parser.py $OPTS || exit 2

cat examples/SWOMCD.txt | python parsers/mcd_parser.py $OPTS || exit 2

cat examples/METAR.txt | python parsers/metar_parser.py $OPTS || exit 2

cat examples/METNC1.txt | python parsers/mos_parser.py $OPTS || exit 2

cat examples/NCR_20121127_1413 | python parsers/nexrad3_attr.py $OPTS || exit 2

cat examples/nldn.bin | python parsers/nldn_parser.py $OPTS || exit 2

cat examples/PIREP.txt | python parsers/pirep_parser.py $OPTS || exit 2

cat examples/RR7.txt | python parsers/rr7.py $OPTS || exit 2

cat examples/SHEF.txt | python parsers/shef_parser.py $OPTS || exit 2

cat examples/PTS.txt | python parsers/spc_parser.py $OPTS || exit 2

cat examples/SPE.txt | python parsers/spe_parser.py $OPTS || exit 2

cat examples/METNC1.txt | python parsers/split_mav.py $OPTS || exit 2

cat examples/SPS.txt | python parsers/sps_parser.py $OPTS || exit 2

cat examples/LSR.txt | python parsers/lsr_parser.py $OPTS || exit 2

gp="WCN WSW TOR TCV FFWTWC_tilde WCNMEG"
for fn in $gp; do
	cat examples/${fn}.txt | python parsers/vtec_parser.py $OPTS || exit 2
done

cat examples/SAW.txt | python parsers/watch_parser.py $OPTS || exit 2
