"""METAR product ingestor

NOTE: It is difficult to keep track of where I am getting the `Metar` library.
So let us document it here for my own sanity.

18 Jul 2017: `snowdepth` branch of my python-metar fork installed with pip
"""

# 3rd Party
from twisted.internet.task import LoopingCall
from twisted.internet import reactor
from pyiem.nws.products import metarcollect
from pyiem.util import get_properties, LOG

# Local
from pywwa import common, SETTINGS
from pywwa.xmpp import make_jabber_client
from pywwa.ldm import bridge
from pywwa.database import get_database, load_metar_stations

IEMDB = get_database("iem")
ASOSDB = get_database("asos")

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
JABBER = make_jabber_client()
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
                LOG.msg("Not adding %s to IGNORELIST as not 4 char id", sid)
                continue
            IGNORELIST.append(sid)
    except Exception as exp:
        LOG.error(exp)
    LOG.info("Updated ignorelist is now %s long", len(IGNORELIST))

    # Call every 15 minutes
    reactor.callLater(15 * 60, load_ignorelist)


def process_data(data):
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
        SETTINGS.get("pywwa_metar_url", "pywwa_metar_url")
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
                    LOG.info("IGNORELIST Jabber relay of %s", jmsg[0])
                    skip = True
        JABBER_MESSAGES.append(jmsg[0])
        if not skip:
            JABBER.send_message(*jmsg)
    if not common.dbwrite_enabled():
        return
    for mtr in collect.metars:
        if mtr.network is None:
            LOG.info(
                "station: '%s' is unknown to metadata table", mtr.station_id
            )
            deffer = ASOSDB.runOperation(
                "INSERT into unknown(id) values (%s)", (mtr.station_id,)
            )
            deffer.addErrback(common.email_error, text)
            continue
        deffer = IEMDB.runInteraction(do_db, mtr)
        deffer.addErrback(common.email_error, collect.unixtext)


def do_db(txn, mtr):
    """Do database transaction"""
    # We always want data to at least go to current_log incase we are getting
    # data out of order :/
    iem, res = mtr.to_iemaccess(txn, force_current_log=True)
    if not res:
        LOG.info(
            "INFO: IEMAccess update of %s returned false: %s",
            iem.data["station"],
            mtr.code,
        )
        df = ASOSDB.runOperation(
            "INSERT into unknown(id, valid) values (%s, %s)",
            (iem.data["station"], iem.data["valid"]),
        )
        df.addErrback(common.email_error, iem.data["station"])


def cleandb():
    """Reset the JABBER_MESSAGES."""
    LOG.info("cleandb() called...")
    JABBER_MESSAGES.clear()
    # Call Again in 1440 minutes
    reactor.callLater(1440, cleandb)


def ready(_):
    """callback once our database load is done"""
    bridge(process_data)
    cleandb()
    load_ignorelist()
    lc = LoopingCall(IEMDB.runInteraction, load_metar_stations, NWSLI_PROVIDER)
    lc.start(720)


def run():
    """Run once at startup"""
    df = IEMDB.runInteraction(load_metar_stations, NWSLI_PROVIDER)
    df.addCallback(ready)
    df.addErrback(LOG.error)
    reactor.run()  # @UndefinedVariable


if __name__ == "__main__":
    run()
