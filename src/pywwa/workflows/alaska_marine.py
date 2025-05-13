"""Alaska CWF/OFF Parser that generates Fake VTEC."""

import click
from psycopg.connection import Transaction
from pyiem.nws.product import TextProduct

from pywwa import LOG, common
from pywwa.database import get_database
from pywwa.ldm import bridge

HEADLINE2VTEC = {
    "BRISK WIND ADVISORY": "BW.Y",
    "GALE WARNING": "GL.W",
    "STORM WARNING": "SR.W",
    "HURRICANE FORCE WIND WARNING": "HF.W",
    "HEAVY FREEZING SPRAY WARNING": "UP.W",
    "SMALL CRAFT ADVISORY": "SC.Y",
}
VTEC_DOMAIN = HEADLINE2VTEC.values()


def get_current(txn: Transaction, nws: TextProduct) -> dict:
    """Get our current entries."""
    wfo = nws.source[1:]
    # VTEC storage does not cross year boundaries, at this time
    table = f"warnings_{nws.valid.year}"
    # Build out the current entries by looking at all UGCs in this product
    current = {}
    ugcdomain = [str(u) for seg in nws.segments for u in seg.ugcs]
    txn.execute(
        f"select ugc, phenomena, significance, eventid from {table}"
        " where wfo = %s and ugc = ANY(%s) and expire > %s and issue <= %s",
        (
            wfo,
            ugcdomain,
            nws.valid,
            nws.valid,
        ),
    )
    for row in txn.fetchall():
        key = f"{row['phenomena']}.{row['significance']}"
        if key not in VTEC_DOMAIN:
            continue
        current[f"{row['ugc']}.{key}"] = row["eventid"]
    return current


def compute_ps(headline: str) -> tuple[str | None, str | None]:
    """Figure out the phenomena and significance from the headline."""
    for key in HEADLINE2VTEC:
        if headline.startswith(key):
            return HEADLINE2VTEC[key].split(".")
    LOG.info("Failed to find PS for %s", headline)
    return None, None


def process_data(txn: Transaction, buf: str) -> TextProduct | None:
    """Actually do something with the buffer, please"""
    if buf.strip() == "":
        return None

    nws = TextProduct(buf, utcnow=common.utcnow())
    wfo = nws.source[1:]
    # VTEC storage does not cross year boundaries, at this time
    table = f"warnings_{nws.valid.year}"

    created_etns = {}
    current = get_current(txn, nws)

    LOG.info(
        "--Processing %s, currently %s entries",
        nws.get_product_id(),
        len(current),
    )
    for segment in nws.segments:
        if len(segment.ugcs) != 1:
            continue
        ugc = segment.ugcs[0]
        ugcexpire = segment.ugcexpire
        for headline in segment.headlines:
            phenomena, significance = compute_ps(headline)
            if phenomena is None:
                continue
            current_key = f"{ugc}.{phenomena}.{significance}"
            # 1. We have already issued, we should extend it
            eventid = current.pop(current_key, None)
            if eventid is not None:
                LOG.info("Extending %s to %s", current_key, ugcexpire)
                txn.execute(
                    f"""
update {table} SET status = 'EXT', expire = %s, updated = %s,
product_ids = array_append(product_ids, %s) WHERE wfo = %s and ugc = %s
and phenomena = %s and significance = %s and eventid = %s
                """,
                    (
                        ugcexpire,
                        nws.valid,
                        nws.get_product_id(),
                        wfo,
                        str(ugc),
                        phenomena,
                        significance,
                        eventid,
                    ),
                )
                continue
            # 2. We have not issued, we should issue
            created_etns_key = f"{phenomena}.{significance}"
            LOG.info("Issuing %s till %s", current_key, ugcexpire)
            if created_etns_key not in created_etns:
                txn.execute(
                    f"""
select coalesce(max(eventid), 0) + 1 as etn from {table}
WHERE wfo = %s and phenomena = %s and significance = %s
                """,
                    (wfo, phenomena, significance),
                )
                row = txn.fetchone()
                etn = row["etn"]
                created_etns[created_etns_key] = etn
            etn = created_etns[created_etns_key]
            txn.execute(
                f"""
INSERT into {table} (issue, expire, updated, wfo, eventid, status,
fcster, ugc, phenomena, significance, gid, init_expire, product_issue,
is_emergency, is_pds, purge_time, product_ids, vtec_year) VALUES
(%s, %s, %s, %s, %s, 'NEW', %s, %s, %s, %s, get_gid(%s, %s),
%s, %s, 'f', 'f', %s, %s, %s)
                """,
                (
                    nws.valid,
                    ugcexpire,
                    nws.valid,
                    wfo,
                    etn,
                    nws.get_signature(),
                    str(ugc),
                    phenomena,
                    significance,
                    str(ugc),
                    nws.valid,
                    ugcexpire,
                    nws.valid,
                    ugcexpire,
                    [nws.get_product_id()],
                    nws.valid.year,
                ),
            )
    for key, eventid in current.items():
        # Cancel
        LOG.info("Canceling [%s]%s", key, eventid)
        (ugc, phenomena, significance) = key.split(".")
        txn.execute(
            f"""
UPDATE {table} SET status = 'CAN', expire = %s, updated = %s,
product_ids = array_append(product_ids, %s)
WHERE wfo = %s and ugc = %s
and phenomena = %s and significance = %s and eventid = %s
""",
            (
                nws.valid,
                nws.valid,
                nws.get_product_id(),
                wfo,
                ugc,
                phenomena,
                significance,
                eventid,
            ),
        )
    return nws


@click.command(help=__doc__)
@common.init
@common.disable_xmpp
def main(*args, **kwargs):
    """Fire up our workflow."""
    # Keep things sequential
    bridge(process_data, dbpool=get_database("postgis", cp_max=1))
