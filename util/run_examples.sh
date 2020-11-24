# Run the various examples through their ingest

echo "afos_dump.py"
cat examples/ESF.txt | python parsers/afos_dump.py || exit 2

echo "aviation.py"
cat examples/SIGC.txt | python parsers/aviation.py || exit 2

echo "cli_parser"
cat examples/CLIANN.txt | python parsers/cli_parser.py || exit 2

echo "dsm2afos.py"
cat examples/DSM.txt | python parsers/dsm2afos.py DSM || exit 2

echo "dsm_parser.py"
cat examples/DSM.txt | python parsers/dsm_parser.py || exit 2

echo "fake_afos_dump.py"
cat examples/CWA.txt | python parsers/fake_afos_dump.py || exit 2

echo "ffg_parser.py"
cat examples/FFGDMX.txt | python parsers/ffg_parser.py || exit 2

echo "generic_parser.py"
gp="AFD ADR AFD2 ADMNFD ADR AT5 VAA TOE"
for fn in $gp; do
	cat examples/${fn}.txt | python parsers/generic_parser.py || exit 2
done

echo "gini2gis.py"
cat examples/TIGH05 | python parsers/gini2gis.py || exit 2

echo "hml_parser.py"
cat examples/HMLARX.txt | python parsers/hml_parser.py || exit 2

echo "lsr_parser.py"
cat examples/LSR.txt | python parsers/lsr_parser.py || exit 2

echo "mcd_parser.py"
cat examples/SWOMCD.txt | python parsers/mcd_parser.py || exit 2

echo "metar_parser.py"
cat examples/METAR.txt | python parsers/metar_parser.py || exit 2

echo "mos_parser.py"
cat examples/METNC1.txt | python parsers/mos_parser.py || exit 2

echo "ncr2postgis.py"
cat examples/NCR_20121127_1413 | python parsers/nexrad3_attr.py || exit 2

echo "nldn_parser.py"
cat examples/nldn.bin | python parsers/nldn_parser.py || exit 2

echo "pirep_parser.py"
cat examples/PIREP.txt | python parsers/pirep_parser.py || exit 2

echo "rr7.py"
cat examples/RR7.txt | python parsers/rr7.py || exit 2

echo "shef_parser.py"
cat examples/SHEF.txt | python parsers/shef_parser.py || exit 2

echo "spc_parser.py"
cat examples/PTS.txt | python parsers/spc_parser.py || exit 2

echo "spe_parser.py"
cat examples/SPE.txt | python parsers/spe_parser.py || exit 2

echo "split_mav.py"
cat examples/METNC1.txt | python parsers/split_mav.py || exit 2

echo "sps_parser.py"
cat examples/SPS.txt | python parsers/sps_parser.py || exit 2

echo "stoia_parser.py"
cat examples/LSR.txt | python parsers/lsr_parser.py || exit 2

echo "vtec_parser.py"
gp="WCN WSW TOR TCV FFWTWC_tilde WCNMEG"
for fn in $gp; do
	cat examples/${fn}.txt | python parsers/vtec_parser.py || exit 2
done

echo "watch_parser.py"
cat examples/SAW.txt | python parsers/watch_parser.py || exit 2
