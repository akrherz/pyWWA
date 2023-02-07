"""
Handle things that need emailed to me for my situational awareness.
"""
# stdlib
from email.mime.text import MIMEText

# 3rd Party
from twisted.internet import reactor
from twisted.mail import smtp
from pyiem.util import LOG
from pyiem.nws import product

# Local
from pywwa import common
from pywwa.ldm import bridge

IOWA_WFOS = ["KDMX", "KDVN", "KARX", "KFSD", "KOAX"]


def process_data(data):
    """
    Actual ingestor
    """
    try:
        real_process(data)
    except Exception as exp:
        common.email_error(exp, data)


def real_process(data) -> product.TextProduct:
    """Go!"""
    prod = product.TextProduct(data)
    if prod.afos == "ADMNES":
        LOG.warning("Dumping %s on the floor", prod.get_product_id())
        return None

    # Strip off stuff at the top
    msg = MIMEText(prod.unixtext[2:], "plain", "utf-8")
    # some products have no AWIPS ID, sigh
    subject = prod.wmo
    msg["To"] = "akrherz@iastate.edu"
    cc = None
    if prod.afos is not None:
        subject = prod.afos
        if prod.afos[:3] == "ADM":
            subject = f"ADMIN NOTICE {prod.afos[3:]}"
        elif prod.afos[:3] == "PNS" and prod.afos != 'PNSWSH':
            if prod.unixtext.upper().find("DAMAGE SURVEY") == -1:
                return None
            subject = f"Damage Survey PNS from {prod.source}"
            cc = "aaron.treadway@noaa.gov"
        elif prod.afos[:3] == "RER":
            subject = f"[RER] {prod.source} {prod.afos[3:]}"
            if prod.source in IOWA_WFOS:
                cc = "Justin.Glisan@iowaagriculture.gov"
    addrs = [
        msg["To"],
    ]
    if cc is not None:
        msg["Cc"] = cc
        addrs.append(cc)
    msg["subject"] = subject
    msg["From"] = common.SETTINGS.get("pywwa_errors_from", "ldm@localhost")
    df = smtp.sendmail(
        common.SETTINGS.get("pywwa_smtp", "smtp"), msg["From"], addrs, msg
    )
    df.addErrback(LOG.error)
    return prod


def main():
    """Go Main Go."""
    common.main(with_jabber=False)
    bridge(process_data)
    reactor.run()


if __name__ == "__main__":
    main()
