"""METAR product ingestor

NOTE: It is difficult to keep track of where I am getting the `Metar` library.
So let us document it here for my own sanity.

18 Jul 2017: `snowdepth` branch of my python-metar fork installed with pip
"""
from __future__ import print_function
import sys

# Twisted Python imports
from twisted.python import log
from twisted.internet import reactor
from pyiem.nws.products import metarcollect
from pyldm import ldmbridge
import common  # @UnresolvedImport

IEMDB = common.get_database("iem")
ASOSDB = common.get_database("asos")

MANUAL = "MANUAL" in sys.argv
NWSLI_PROVIDER = {}
# Manual list of sites that are sent to jabber :/
metarcollect.JABBER_SITES = {
    "KJFK": None,
    "KLGA": None,
    "KEWR": None,
    "KTEB": None,
    "KIAD": None,
    "KDCA": None,
    "KBWI": None,
    "KRIC": None,
    "KPHL": None,
}


def load_stations(txn):
    """load station metadata to build a xref of stations to networks"""
    txn.execute(
        """
        SELECT *, ST_X(geom) as lon, ST_Y(geom) as lat from stations
        where network ~* 'ASOS' or network = 'AWOS' or network = 'WTM'
    """
    )
    news = 0
    for row in txn.fetchall():
        if row["id"] not in NWSLI_PROVIDER:
            news += 1
            NWSLI_PROVIDER[row["id"]] = row

    log.msg("Loaded %s new stations" % (news,))
    # Reload every 12 hours
    reactor.callLater(
        12 * 60 * 60, IEMDB.runInteraction, load_stations  # @UndefinedVariable
    )


def shutdown():
    """Shut this down, gracefully"""
    reactor.callWhenRunning(reactor.stop)  # @UndefinedVariable


class MyProductIngestor(ldmbridge.LDMProductReceiver):
    """Our LDM pqact product receiver"""

    def connectionLost(self, reason):
        """The connection was lost for some reason"""
        log.msg("connectionLost")
        log.err(reason)
        reactor.callLater(30, shutdown)  # @UndefinedVariable

    def process_data(self, data):
        """Callback when we have data to process"""
        try:
            # BUG make sure we are okay here after we resolve pyLDM str issues
            real_processor(data)
        except Exception as exp:
            common.email_error(exp, data, -1)


def real_processor(text):
    """Process this product, please"""
    collect = metarcollect.parser(text, nwsli_provider=NWSLI_PROVIDER)
    if collect.warnings:
        common.email_error("\n".join(collect.warnings), collect.unixtext)
    jmsgs = collect.get_jabbers(
        ("https://mesonet.agron.iastate.edu/ASOS/" "current.phtml?network=")
    )
    if not MANUAL:
        for jmsg in jmsgs:
            JABBER.send_message(*jmsg)
    for mtr in collect.metars:
        if mtr.network is None:
            log.msg(
                ("station: '%s' is unknown to metadata table")
                % (mtr.station_id,)
            )
            deffer = ASOSDB.runOperation(
                """
            INSERT into unknown(id) values (%s)
            """,
                (mtr.station_id,),
            )
            deffer.addErrback(common.email_error, text)
            continue
        deffer = IEMDB.runInteraction(do_db, mtr)
        deffer.addErrback(common.email_error, collect.unixtext)


def do_db(txn, mtr):
    """Do database transaction"""
    # We always want data to at least go to current_log incase we are getting
    # data out of order :/
    iem, res = mtr.to_iemaccess(
        txn, force_current_log=True, skip_current=MANUAL
    )
    if not res:
        log.msg(
            ("INFO: IEMAccess update of %s returned false: %s")
            % (iem.data["station"], mtr.code)
        )
        df = ASOSDB.runOperation(
            """
            INSERT into unknown(id, valid)
            values (%s, %s)
        """,
            (iem.data["station"], iem.data["valid"]),
        )
        df.addErrback(common.email_error, iem.data["station"])


def ready(_):
    """callback once our database load is done"""
    ingest = MyProductIngestor()
    ldmbridge.LDMProductFactory(ingest)


def run():
    """Run once at startup"""
    df = IEMDB.runInteraction(load_stations)
    df.addCallback(ready)
    reactor.run()  # @UndefinedVariable


if __name__ == "__main__":
    JABBER = common.make_jabber_client("metar_parser")
    run()
