"""Dump some stuff without AFOS PILs"""
# Stdlib
import re

from pyiem.nws.product import TextProduct

# 3rd Party
from twisted.internet import reactor

# Local
from pywwa import common
from pywwa.database import get_database
from pywwa.ldm import bridge

CWA = re.compile("^FA(AK|HI|US)2([1-6])$")
MIS = re.compile("^FA(AK|HI|US)20$")
GMET = {
    "LWGE86": "GMTIFR",
    "LWHE00": "GMTTRB",
    "LWIE00": "GMTICE",
}
# https://www.nhc.noaa.gov/archive/recon/readme.txt
NHC = {
    "URPA15": "AHOPA1",
    "URPN15": "AHOPN1",
    "URNT15": "AHONT1",
    "URNT10": "REPNT0",
    "URNT11": "REPNT1",
    "URNT12": "REPNT2",
    "URNT13": "REPNT3",
    "URNT14": "REPNTS",
    "URPA10": "REPPA0",
    "URPA11": "REPPA1",
    "URPA12": "REPPA2",
    "URPA13": "REPPA3",
    "URPN10": "REPPN0",
    "URPN11": "REPPN1",
    "URPN12": "REPPN2",
    "URPN13": "REPPN3",
    "URPN14": "REPPNS",
}


def compute_afos(textprod):
    """Our hackery to assign a fake AFOS pil to a product without AFOS"""
    ttaaii = textprod.wmo
    if ttaaii[:4] == "NOXX":
        afos = f"ADM{textprod.source[1:]}"
    elif ttaaii in GMET:
        afos = GMET[ttaaii]
    elif MIS.match(ttaaii):
        afos = f"MIS{textprod.source[1:]}"
    elif CWA.match(ttaaii):
        afos = f"CWA{textprod.source[1:]}"
    elif ttaaii[:4] == "FOUS":
        afos = f"FRH{ttaaii[4:]}"
    elif ttaaii[:2] == "FO" and ttaaii[2:4] in [
        "CA",
        "UE",
        "UM",
        "CN",
        "GX",
        "UW",
    ]:
        afos = f"FRHT{ttaaii[4:]}"
    elif textprod.source in ["KNHC", "KWBC"] and ttaaii in NHC:
        afos = NHC[ttaaii]
    else:
        raise Exception(f"Unknown TTAAII {ttaaii} conversion")

    textprod.afos = afos


def really_process_data(txn, data):
    """We are called with a hard coded AFOS PIL"""
    tp = TextProduct(data, utcnow=common.utcnow(), parse_segments=False)
    if tp.afos is None:
        compute_afos(tp)

    sql = (
        "INSERT into products "
        "(pil, data, source, wmo, entered, bbb) values(%s,%s,%s,%s,%s,%s)"
    )

    sqlargs = (
        tp.afos,
        tp.text,
        tp.source,
        tp.wmo,
        tp.valid.strftime("%Y-%m-%d %H:%M+00"),
        tp.bbb,
    )
    if common.dbwrite_enabled():
        txn.execute(sql, sqlargs)

    # CWA is handled by cwa_parser.py
    if tp.afos[:3] in ["FRH", "CWA"]:
        return tp
    jmsgs = tp.get_jabbers(
        common.SETTINGS.get("pywwa_product_url", "pywwa_product_url")
    )
    for jmsg in jmsgs:
        common.send_message(*jmsg)
    return tp


def main():
    """Go Main Go."""
    common.main()
    bridge(really_process_data, dbpool=get_database("afos"))
    reactor.run()


if __name__ == "__main__":
    main()
