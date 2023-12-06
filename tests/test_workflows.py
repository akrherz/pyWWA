"""Test pywwa.workflows."""
# stdlib
import importlib

import mock

# 3rd Party
import pytest
from twisted.internet import reactor

# Local
reactor.run = mock.Mock()
_parsers = [
    "afos_dump",
    "bufr_surface",
    "dsm_parser",
    "hml_parser",
    "mos_parser",
    "spe_parser",
    "aviation",
    "fake_afos_dump",
    "nexrad3_attr",
    "scp_parser",
    "split_mav",
    "cf6_parser",
    "ffg_parser",
    "lsr_parser",
    "nldn_parser",
    "shef_parser",
    "sps_parser",
    "cli_parser",
    "generic_parser",
    "mcd_parser",
    "pirep_parser",
    "spammer",
    "vtec_parser",
    "metar_parser",
    "spc_parser",
    "watch_parser",
]


@pytest.mark.parametrize("parser", _parsers)
def test_main(parser):
    """Exercise the main functions."""
    name = f"pywwa.workflows.{parser}"
    app = importlib.import_module(name)
    app.main()
