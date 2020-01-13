"""
Handle things that need emailed to me for my situational awareness.
"""
from email.mime.text import MIMEText

from twisted.internet import reactor
from twisted.python import log
from twisted.mail import smtp
from pyldm import ldmbridge
from pyiem.nws import product
import common  # @UnresolvedImport

IOWA_WFOS = ["KDMX", "KDVN", "KARX", "KFSD", "KOAX"]


class MyProductIngestor(ldmbridge.LDMProductReceiver):
    """Do what we do!"""

    prods = 0

    def process_data(self, data):
        """
        Actual ingestor
        """
        self.prods += 1
        try:
            real_process(data)
        except Exception as exp:
            common.email_error(exp, data)

    def connectionLost(self, reason):
        """
        Called when ldm closes the pipe
        """
        log.msg("processed %s prods" % (self.prods,))
        reactor.callLater(5, shutdown)


def shutdown():
    """Shutme off"""
    reactor.callWhenRunning(reactor.stop)


def real_process(data):
    """Go!"""
    prod = product.TextProduct(data)
    if prod.afos == "ADMNES":
        log.msg("Dumping %s on the floor" % (prod.get_product_id(),))
        return

    # Strip off stuff at the top
    msg = MIMEText(prod.unixtext[2:], "plain", "utf-8")
    # some products have no AWIPS ID, sigh
    subject = prod.wmo
    if prod.afos is not None:
        subject = prod.afos
        if prod.afos[:3] == "ADM":
            subject = "ADMIN NOTICE %s" % (prod.afos[3:],)
        elif prod.afos[:3] == "RER":
            subject = "[RER] %s %s" % (prod.source, prod.afos[3:])
            if prod.source in IOWA_WFOS:
                msg["Cc"] = "Justin.Glisan@iowaagriculture.gov"
    msg["subject"] = subject
    msg["From"] = common.SETTINGS.get("pywwa_errors_from", "ldm@localhost")
    msg["To"] = "akrherz@iastate.edu"
    df = smtp.sendmail(
        common.SETTINGS.get("pywwa_smtp", "smtp"), msg["From"], msg["To"], msg
    )
    df.addErrback(log.err)


if __name__ == "__main__":
    ldmbridge.LDMProductFactory(MyProductIngestor())
    reactor.run()
