#!/bin/bash
# Run the various examples through their ingest

echo "afos_dump.py"
cat examples/ESF.txt | python parsers/afos_dump.py || echo "afos Error $fn"

echo "aviation.py"
cat examples/SIGC.txt | python parsers/aviation.py || echo "aviation Error $fn"

echo "cli_parser"
cat examples/CLIANN.txt | python parsers/cli_parser.py || echo "CLI Error $fn"

echo "dsm2afos.py"
cat examples/DSM.txt | python parsers/dsm2afos.py DSM || echo "DSM2 Error $fn"

echo "dsm_parser.py"
cat examples/DSM.txt | python parsers/dsm_parser.py || echo "DSM Error $fn"

echo "fake_afos_dump.py"
cat examples/CWA.txt | python parsers/fake_afos_dump.py || echo "FAKE Error $fn"

echo "ffg_parser.py"
cat examples/FFGDMX.txt | python parsers/ffg_parser.py || echo "FFG Error $fn"

echo "generic_parser.py"
gp="AFD ADR AFD2 ADMNFD ADR AT5 VAA TOE"
for fn in $gp; do
	cat examples/${fn}.txt | python parsers/generic_parser.py || echo "GP Error $fn"
done

echo "gini2gis.py"
cat examples/TIGH05 | python parsers/gini2gis.py || echo "GINI Error $fn"

echo "hml_parser.py"
cat examples/HMLARX.txt | python parsers/hml_parser.py || echo "HML Error $fn"

echo "lsr_parser.py"
cat examples/LSR.txt | python parsers/lsr_parser.py || echo "LSR Error $fn"

echo "mcd_parser.py"
cat examples/SWOMCD.txt | python parsers/mcd_parser.py || echo "MCD Error $fn"

echo "metar_parser.py"
cat examples/METAR.txt | python parsers/metar_parser.py || echo "METAR Error $fn"

echo "mos_parser.py"
cat examples/METNC1.txt | python parsers/mos_parser.py || echo "MOS Error $fn"

echo "ncr2postgis.py"
cat examples/NCR_20121127_1413 | python parsers/nexrad3_attr.py || echo "NCR Error $fn"

echo "nldn_parser.py"
cat examples/nldn.bin | python parsers/nldn_parser.py || echo "NLDN Error $fn"

echo "pirep_parser.py"
cat examples/PIREP.txt | python parsers/pirep_parser.py || echo "PIREP Error $fn"

echo "rr7.py"
cat examples/RR7.txt | python parsers/rr7.py || echo "RR7 Error $fn"

echo "shef_parser.py"
cat examples/SHEF.txt | python parsers/shef_parser.py || echo "SHEF Error $fn"

echo "spc_parser.py"
cat examples/PTS.txt | python parsers/spc_parser.py || echo "SPC Error $fn"

echo "spe_parser.py"
cat examples/SPE.txt | python parsers/spe_parser.py || echo "SPE Error $fn"

echo "split_mav.py"
cat examples/METNC1.txt | python parsers/split_mav.py || echo "Split MAV Error $fn"

echo "sps_parser.py"
cat examples/SPS.txt | python parsers/sps_parser.py || echo "SPS Error $fn"

echo "stoia_parser.py"
cat examples/LSR.txt | python parsers/lsr_parser.py || echo "LSR Error $fn"

echo "vtec_parser.py"
gp="WCN WSW TOR TCV FFWTWC_tilde WCNMEG"
for fn in $gp; do
	cat examples/${fn}.txt | python parsers/vtec_parser.py || echo "vtec Error $fn"
done

echo "watch_parser.py"
cat examples/SAW.txt | python parsers/watch_parser.py || echo "watch Error $fn"
