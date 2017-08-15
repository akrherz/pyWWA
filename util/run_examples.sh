#!/bin/bash
# Run the various examples through their ingest

echo "afos_dump.py"
cat examples/ESF.txt | python parsers/afos_dump.py || echo "afos Error $fn"

echo "aviation.py"
cat examples/SIGC.txt | python parsers/aviation.py || echo "aviation Error $fn"

echo "cli_parser"
cat examples/CLIANN.txt | python parsers/cli_parser.py || echo "CLI Error $fn"

echo "dsm2afos.py"
echo "dsm_parser.py"
echo "fake_afos_dump.py"
echo "ffg_parser.py"

echo "generic_parser.py"
gp="AFD AFD2 ADMNFD ADR AT5 VAA TOE"
for fn in $gp; do
	cat examples/${fn}.txt | python parsers/generic_parser.py || echo "GP Error $fn"
done

echo "gini2gis.py"
echo "hml_parser.py"
echo "lsr_parser.py"
cat examples/LSR.txt | python parsers/lsr_parser.py || echo "LSR Error $fn"

echo "mcd_parser.py"
cat examples/SWOMCD.txt | python parsers/mcd_parser.py || echo "MCD Error $fn"

echo "metar_parser.py"
echo "mos_parser.py"
echo "ncr2postgis.py"
echo "nexrad3_attr.py"
echo "nldn_parser.py"
echo "pirep_parser.py"
echo "rr7.py"
echo "shef_parser.py"

echo "spc_parser.py"
cat examples/PTSDY1.txt | python parsers/spc_parser.py || echo "SPC Error $fn"

echo "spe_parser.py"
echo "split_mav.py"
echo "sps_parser.py"
cat examples/SPS.txt | python parsers/sps_parser.py || echo "SPS Error $fn"

echo "stoia_parser.py"

echo "vtec_parser.py"
gp="WCN WSW TOR TCV"
for fn in $gp; do
	cat examples/${fn}.txt | python parsers/vtec_parser.py || echo "vtec Error $fn"
done

echo "watch_parser.py"
cat examples/SAW.txt | python parsers/watch_parser.py || echo "watch Error $fn"
