""" NLDN """
# stdlib
from io import BytesIO

from pyiem.nws.products.nldn import parser

# 3rd Party
from twisted.internet import reactor

# Local
from pywwa import common
from pywwa.database import get_database
from pywwa.ldm import bridge

DBPOOL = get_database("nldn")


def process_data(data):
    """Actual ingestor"""
    if data == b"":
        return
    real_process(data)
    try:
        real_process(data)
    except Exception as myexp:
        common.email_error(myexp, data)


def real_process(buf):
    """The real processor of the raw data, fun!"""
    np = parser(BytesIO(b"NLDN" + buf))
    if common.dbwrite_enabled():
        DBPOOL.runInteraction(np.sql)


def main():
    """Go Main"""
    common.main(with_jabber=False)
    bridge(process_data, isbinary=True, product_end=b"NLDN")
    reactor.run()  # @UndefinedVariable


if __name__ == "__main__":
    main()
