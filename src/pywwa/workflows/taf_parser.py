""" TAF Ingestor """

# 3rd Party
from pyiem.nws.products.taf import parser
from twisted.internet import reactor

# Local
from pywwa import common
from pywwa.database import get_database
from pywwa.ldm import bridge


def real_process(txn, raw):
    """Process the product, please"""
    prod = parser(raw, utcnow=common.utcnow())
    if common.dbwrite_enabled():
        prod.sql(txn)
    baseurl = common.SETTINGS.get("pywwa_product_url", "pywwa_product_url")
    jmsgs = prod.get_jabbers(baseurl)
    for mess, htmlmess, xtra in jmsgs:
        common.send_message(mess, htmlmess, xtra)
    if prod.warnings:
        common.email_error("\n\n".join(prod.warnings), prod.text)


def main():
    """Go Main Go"""
    common.main()
    bridge(real_process, dbpool=get_database("asos"))
    reactor.run()  # @UndefinedVariable


if __name__ == "__main__":
    main()
