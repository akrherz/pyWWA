"""Test awos_ingest."""

import pywwa
from pyiem.util import utc
from pywwa.workflows import awos_ingest


def test_api():
    """Test that we can load things to ignore."""
    pywwa.CTX.utcnow = utc(2023, 8, 9, 16, 56)
    awos_ingest.ready(None)
    mtr = "KVTI 091535Z AUTO 26009G15KT 10SM SCT030 03/00 A2976 RMK AO2"
    awos_ingest.process_line(mtr)
