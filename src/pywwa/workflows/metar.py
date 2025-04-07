"""METAR product ingestor

NOTE: It is difficult to keep track of where I am getting the `Metar` library.
So let us document it here for my own sanity.

18 Jul 2017: `snowdepth` branch of my python-metar fork installed with pip
"""

# stdlib
from datetime import timezone

import click
from pyiem.nws.products import metarcollect
from pyiem.util import get_properties
from twisted.internet import reactor
from twisted.internet.task import LoopingCall

# Local
from pywwa import LOG, SETTINGS, common
from pywwa.database import get_database, load_metar_stations
from pywwa.ldm import bridge

IEMDB = get_database("iem")
ASOSDB = get_database("asos")
MESOSITEDB = get_database("mesosite")

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
# Sites with higher thresholds, hardcoded here for now :/
metarcollect.WIND_ALERT_THRESHOLD_KTS_BY_ICAO["KMWN"] = 80
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
        return real_processor(data)
    except Exception as exp:
        common.email_error(exp, data, -1)
    return None


def real_processor(text):
    """Process this product, please"""
    collect = metarcollect.parser(
        text,
        utcnow=common.utcnow(),
        nwsli_provider=NWSLI_PROVIDER,
        ugc_provider={},
    )
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
            if (
                channel.startswith("METAR.")
                and channel.split(".")[1] in IGNORELIST
            ):
                LOG.info("IGNORELIST Jabber relay of %s", jmsg[0])
                skip = True
        JABBER_MESSAGES.append(jmsg[0])
        if not skip:
            common.send_message(*jmsg)
    if not common.dbwrite_enabled():
        return collect
    for mtr in collect.metars:
        key = metarcollect.normid(mtr.station_id)
        entry = NWSLI_PROVIDER.get(key)
        if entry is None:
            LOG.info("station: '%s' is unknown to metadata table", key)
            deffer = ASOSDB.runOperation(
                "INSERT into unknown(id, valid) values (%s, %s)",
                (mtr.station_id, mtr.time.replace(tzinfo=timezone.utc)),
            )
            deffer.addErrback(common.email_error, text)
            continue
        deffer = IEMDB.runInteraction(do_db, mtr, entry)
        deffer.addErrback(common.email_error, collect.unixtext)
    return collect


def do_db(txn, mtr, metadata: dict):
    """Do database transaction"""
    # We always want data to at least go to current_log incase we are getting
    # data out of order :/
    iem, res = metarcollect.to_iemaccess(
        txn,
        mtr,
        iemid=metadata["iemid"],
        tzname=metadata["tzname"],
        force_current_log=True,
    )
    if not res:
        LOG.info(
            "INFO: IEMAccess update of %s returned false: %s",
            metadata["id"],
            mtr.code,
        )
        df = ASOSDB.runOperation(
            "INSERT into unknown(id, valid) values (%s, %s)",
            (mtr.station_id, iem.data["valid"]),
        )
        df.addErrback(common.email_error, mtr.station_id)


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
    lc = LoopingCall(
        MESOSITEDB.runInteraction, load_metar_stations, NWSLI_PROVIDER
    )
    lc.start(720, now=False)


@click.command(help=__doc__)
@common.init
def main(*args, **kwargs):
    """Run once at startup"""
    df = MESOSITEDB.runInteraction(load_metar_stations, NWSLI_PROVIDER)
    df.addCallback(ready)
    df.addErrback(LOG.error)
