# Run the various examples through their ingest
set -x
set -e
OPTS="-l -x -s 1 -e"

cat examples/FD1US1.txt | python parsers/fd_parser.py $OPTS -u 2023-03-09T12:00 || exit 2

cat examples/XTEUS.txt | python parsers/xteus_parser.py $OPTS || exit 2

cat examples/ESF.txt | python parsers/afos_dump.py $OPTS || exit 2

cat examples/SIGC.txt | python parsers/aviation.py $OPTS || exit 2

cat examples/CF6.txt | python parsers/cf6_parser.py $OPTS || exit 2

cat examples/CLIANN.txt | python parsers/cli_parser.py $OPTS || exit 2

cat examples/CWA.txt | python parsers/cwa_parser.py $OPTS -u 2022-03-03T12:00 || exit 2

cat examples/DSM.txt | python parsers/dsm2afos.py || exit 2

cat examples/DSM.txt | python parsers/dsm_parser.py $OPTS || exit 2

cat examples/CWA.txt | python parsers/fake_afos_dump.py $OPTS || exit 2

cat examples/LWGE86.txt | python parsers/fake_afos_dump.py $OPTS || exit 2

cat examples/FFGDMX.txt | python parsers/ffg_parser.py $OPTS || exit 2

gp="AFD ADR AFD2 ADMNFD ADR AT5 VAA TOE"
for fn in $gp; do
    cat examples/${fn}.txt | python parsers/generic_parser.py $OPTS || exit 2
done

cat examples/HMLARX.txt | python parsers/hml_parser.py $OPTS || exit 2

cat examples/LSR.txt | python parsers/lsr_parser.py $OPTS || exit 2

cat examples/SWOMCD.txt | python parsers/mcd_parser.py $OPTS || exit 2

cat examples/METAR.txt | python parsers/metar_parser.py $OPTS || exit 2

cat examples/METNC1.txt | python parsers/mos_parser.py $OPTS || exit 2

cat examples/NCR_20121127_1413 | python parsers/nexrad3_attr.py $OPTS || exit 2

python parsers/nldn_parser.py --help

cat examples/PIREP.txt | python parsers/pirep_parser.py $OPTS || exit 2

cat examples/SHEF/RR7ZOB.txt | python parsers/rr7.py $OPTS || exit 2

cat examples/SHEF.txt | python parsers/shef_parser.py $OPTS || exit 2

cat examples/PTS.txt | python parsers/spc_parser.py $OPTS || exit 2

cat examples/SPE.txt | python parsers/spe_parser.py $OPTS -u 2011-10-31T01:16 || exit 2

cat examples/METNC1.txt | python parsers/split_mav.py $OPTS || exit 2

cat examples/SPS.txt | python parsers/sps_parser.py $OPTS || exit 2

gp="WCN WSW TOR TCV FFWTWC_tilde WCNMEG"
for fn in $gp; do
	cat examples/${fn}.txt | python parsers/vtec_parser.py $OPTS || exit 2
done

cat examples/SAW.txt examples/WWP9.txt | python parsers/watch_parser.py $OPTS || exit 2

cat examples/TAF.txt | python parsers/taf_parser.py $OPTS || exit 2

cat examples/RBG | python parsers/ero_parser.py $OPTS || exit 2
