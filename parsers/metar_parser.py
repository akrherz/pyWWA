"""METAR product ingestor

NOTE: It is difficult to keep track of where I am getting the `Metar` library.
So let us document it here for my own sanity.

18 Jul 2017: `snowdepth` branch of my python-metar fork installed with pip
"""

# Twisted Python imports
from twisted.python import log
from twisted.internet import reactor
from pyiem.nws.products import metarcollect
from pyiem.util import get_properties
from pyldm import ldmbridge
import common  # @UnresolvedImport

IEMDB = common.get_database("iem")
ASOSDB = common.get_database("asos")

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
# Try to prevent Jabber message dups
JABBER_MESSAGES = []
# List of sites to IGNORE and not send Jabber Messages for.
# iem property `pywwa_metar_ignorelist` should be a comma delimited 4 char ids
IGNORELIST = []


def load_ignorelist():
    """Sync what the properties database has for sites to ignore."""
    try:
        prop = get_properties().get("pywwa_metar_ignorelist", "")
        IGNORELIST.clear()
        for sid in [x.strip() for x in prop.split(",")]:
            if sid == "":
                continue
            if len(sid) != 4:
                log.msg(f"Not adding {sid} to IGNORELIST as not 4 char id")
                continue
            IGNORELIST.append(sid)
    except Exception as exp:
        log.err(exp)
    log.msg(f"Updated ignorelist is now {len(IGNORELIST)} long")

    # Call every 15 minutes
    reactor.callLater(15 * 60, load_ignorelist)


def load_stations(txn):
    """load station metadata to build a xref of stations to networks"""
    txn.execute(
        "SELECT *, ST_X(geom) as lon, ST_Y(geom) as lat from stations "
        "where network ~* 'ASOS' or network = 'AWOS'"
    )
    news = 0
    # Need the fetchall due to non-async here
    for row in txn.fetchall():
        if row["id"] not in NWSLI_PROVIDER:
            news += 1
            NWSLI_PROVIDER[row["id"]] = row

    log.msg(f"Loaded {news} new stations")
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
            real_processor(data)
        except Exception as exp:
            common.email_error(exp, data, -1)


def real_processor(text):
    """Process this product, please"""
    collect = metarcollect.parser(text, nwsli_provider=NWSLI_PROVIDER)
    if collect.warnings:
        common.email_error("\n".join(collect.warnings), collect.unixtext)
    jmsgs = collect.get_jabbers(
        "https://mesonet.agron.iastate.edu/ASOS/current.phtml?network="
    )
    for jmsg in jmsgs:
        if jmsg[0] in JABBER_MESSAGES:
            continue
        # Hacky here, but get the METAR.XXXX channel to find which site
        # this is.
        skip = False
        channels = jmsg[2].get("channels", [])
        for channel in channels.split(","):
            if channel.startswith("METAR."):
                if channel.split(".")[1] in IGNORELIST:
                    log.msg(f"IGNORELIST Jabber relay of {jmsg[0]}")
                    skip = True
        JABBER_MESSAGES.append(jmsg[0])
        if not skip:
            JABBER.send_message(*jmsg)
    for mtr in collect.metars:
        if mtr.network is None:
            log.msg("station: '{mtr.station_id}' is unknown to metadata table")
            deffer = ASOSDB.runOperation(
                "INSERT into unknown(id) values (%s)", (mtr.station_id,)
            )
            deffer.addErrback(common.email_error, text)
            continue
        if common.dbwrite_enabled():
            deffer = IEMDB.runInteraction(do_db, mtr)
            deffer.addErrback(common.email_error, collect.unixtext)


def do_db(txn, mtr):
    """Do database transaction"""
    # We always want data to at least go to current_log incase we are getting
    # data out of order :/
    iem, res = mtr.to_iemaccess(txn, force_current_log=True)
    if not res:
        log.msg(
            ("INFO: IEMAccess update of %s returned false: %s")
            % (iem.data["station"], mtr.code)
        )
        df = ASOSDB.runOperation(
            "INSERT into unknown(id, valid) values (%s, %s)",
            (iem.data["station"], iem.data["valid"]),
        )
        df.addErrback(common.email_error, iem.data["station"])


def cleandb():
    """Reset the JABBER_MESSAGES."""
    log.msg("cleandb() called...")
    JABBER_MESSAGES.clear()
    # Call Again in 1440 minutes
    reactor.callLater(1440, cleandb)


def ready(_):
    """callback once our database load is done"""
    ingest = MyProductIngestor()
    ldmbridge.LDMProductFactory(ingest)
    cleandb()
    load_ignorelist()


def run():
    """Run once at startup"""
    df = IEMDB.runInteraction(load_stations)
    df.addCallback(ready)
    reactor.run()  # @UndefinedVariable


if __name__ == "__main__":
    JABBER = common.make_jabber_client("metar_parser")
    run()
