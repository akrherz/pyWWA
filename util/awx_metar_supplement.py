"""Repair holes when aviationweather.gov METAR service has data."""

import subprocess
import sys
import time

import click
import httpx
import pandas as pd
from pyiem.database import get_sqlalchemy_conn
from pyiem.util import logger, utc
from sqlalchemy import text
from tqdm import tqdm

LOG = logger()
FN = "/mesonet/tmp/AWX_METARS.txt"


@click.command()
@click.option("--network", help="Network to query, defaults to all.")
def main(network):
    """Go Main Go."""
    nlim = "network ~* 'ASOS'" if network is None else "network = :network"
    with get_sqlalchemy_conn("mesosite") as conn:
        # don't do online check as awx may have data we don't
        stations = pd.read_sql(
            text(
                f"select iemid, id, network from stations where {nlim} "
                "order by id ASC"
            ),
            conn,
            params={"network": network},
            index_col="iemid",
        )
    stations["found"] = 0
    with (
        open(FN, "wb") as fh,
        httpx.Client() as client,
        get_sqlalchemy_conn("iem") as conn,
    ):
        # create faked noaaport header
        fh.write(b"000 \r\r\n")
        fh.write(f"SAUS70 KISU {utc():%d%H%M}\r\r\n".encode("ascii"))
        for iemid, row in tqdm(
            stations.iterrows(),
            disable=not sys.stdout.isatty(),
            total=len(stations.index),
        ):
            st4 = row["id"] if len(row["id"]) == 4 else f"K{row['id']}"
            url = (
                "https://aviationweather.gov/cgi-bin/data/metar.php?"
                f"ids={st4}&hours=48&order=id%2C-obs&sep=true"
            )
            attempt = 0
            while attempt < 3:
                attempt += 1
                try:
                    req = client.get(url, timeout=20)
                    if req.status_code == 429:
                        LOG.info("Got 429, cooling jets for 5 seconds.")
                        time.sleep(5)
                        continue
                    if req.status_code != 200:
                        LOG.warning(f"Failed to fetch {st4} {req.status_code}")
                        continue
                    break
                except Exception as exp:
                    LOG.info("Failed to fetch %s: %s", st4, exp)
            awx = {}
            for line in req.text.split("\n"):
                if line.strip() == "":
                    continue
                awx[line[5:11]] = f"{line}="
            res = conn.execute(
                text(
                    "select valid at time zone 'UTC' as v from current_log "
                    "WHERE iemid = :iemid and "
                    "valid > now() - '50 hours'::interval "
                    "and raw !~* 'MADISHF' "
                    "ORDER by valid ASC"
                ),
                {"iemid": iemid},
            )

            for row2 in res:
                key = f"{row2[0]:%d%H%M}"
                if key in awx:
                    awx.pop(key)
            if awx:
                stations.at[iemid, "found"] = len(awx)
                for _, val in awx.items():
                    fh.write(f"{val}\r\r\n".encode("ascii"))
            # throttle requests to something around 5/s
            time.sleep(0.2)
        fh.write(b"\003")

    LOG.info("Found %s METARs", stations["found"].sum())
    with subprocess.Popen(
        [
            "pywwa-parse-metar",
            "-x",
            "-s",
            "120",
        ],
        stdin=subprocess.PIPE,
    ) as proc:
        with open(FN, "rb") as fh:
            proc.stdin.write(fh.read())
        proc.stdin.close()
        proc.wait()


if __name__ == "__main__":
    main()
