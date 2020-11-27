"""Dump some stuff without AFOS PILs"""

# 3rd Party
from twisted.internet import reactor
from pyiem.nws.product import TextProduct

# Local
from pywwa import common
from pywwa.xmpp import make_jabber_client
from pywwa.ldm import bridge
from pywwa.database import get_database

JABBER = make_jabber_client()


def compute_afos(textprod):
    """Our hackery to assign a fake AFOS pil to a product without AFOS"""
    ttaaii = textprod.wmo
    if ttaaii[:4] == "NOXX":
        afos = "ADM%s" % (textprod.source[1:],)
    elif ttaaii == "FAUS20":
        afos = "MIS%s" % (textprod.source[1:],)
    elif ttaaii in [
        "FAUS21",
        "FAUS22",
        "FAUS23",
        "FAUS24",
        "FAUS25",
        "FAUS26",
    ]:
        afos = "CWA%s" % (textprod.source[1:],)
    elif ttaaii[:4] == "FOUS":
        afos = "FRH%s" % (ttaaii[4:],)
    elif ttaaii[:2] == "FO" and ttaaii[2:4] in [
        "CA",
        "UE",
        "UM",
        "CN",
        "GX",
        "UW",
    ]:
        afos = "FRHT%s" % (ttaaii[4:],)
    elif ttaaii == "URNT12" and textprod.source in ["KNHC", "KWBC"]:
        afos = "REPNT2"
    else:
        raise Exception("Unknown TTAAII %s conversion" % (ttaaii,))

    textprod.afos = afos


def really_process_data(txn, data):
    """ We are called with a hard coded AFOS PIL """
    tp = TextProduct(data)
    if tp.afos is None:
        compute_afos(tp)

    sql = (
        "INSERT into products "
        "(pil, data, source, wmo, entered) values(%s,%s,%s,%s,%s)"
    )

    sqlargs = (
        tp.afos,
        tp.text,
        tp.source,
        tp.wmo,
        tp.valid.strftime("%Y-%m-%d %H:%M+00"),
    )
    if common.dbwrite_enabled():
        txn.execute(sql, sqlargs)

    if tp.afos[:3] == "FRH":
        return
    jmsgs = tp.get_jabbers(
        common.SETTINGS.get("pywwa_product_url", "pywwa_product_url")
    )
    for jmsg in jmsgs:
        JABBER.send_message(*jmsg)


if __name__ == "__main__":
    bridge(really_process_data, dbpool=get_database("afos"))
    reactor.run()
