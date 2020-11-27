"""Database utilities."""
# stdlib
import re

# 3rd Party
from pyiem.util import LOG
from pyiem.nws import ugc
from pyiem.nws import nwsli

# Local
from pywwa.common import get_sync_dbconn


def load_ugcs_nwsli(ugc_dict, nwsli_dict):
    """Synchronous load of metadata tables."""
    with get_sync_dbconn("postgis") as pgconn:
        cursor = pgconn.cursor()
        cursor.execute(
            "SELECT name, ugc, wfo from ugcs WHERE name IS NOT null and "
            "begin_ts < now() and (end_ts is null or end_ts > now())"
        )
        for row in cursor:
            nm = (row[0]).replace("\x92", " ").replace("\xc2", " ")
            wfos = re.findall(r"([A-Z][A-Z][A-Z])", row[2])
            ugc_dict[row[1]] = ugc.UGC(
                row[1][:2], row[1][2], row[1][3:], name=nm, wfos=wfos
            )

        LOG.info("ugc_dict loaded %s entries", len(ugc_dict))

        cursor.execute(
            "SELECT nwsli, river_name || ' ' || proximity || ' ' || "
            "name || ' ['||state||']' as rname from hvtec_nwsli"
        )
        for row in cursor:
            nm = row[1].replace("&", " and ")
            nwsli_dict[row[0]] = nwsli.NWSLI(row[0], name=nm)

        LOG.info("nwsli_dict loaded %s entries", len(nwsli_dict))
