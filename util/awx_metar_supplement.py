"""Repair holes when aviationweather.gov METAR service has data."""

import subprocess
import sys
import time

import click
import httpx
import pandas as pd
from pyiem.database import get_sqlalchemy_conn, sql_helper
from pyiem.util import logger, utc
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
            sql_helper(
                "select iemid, id, network from stations where {nlim} "
                "order by id ASC",
                nlim=nlim,
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
        progress = tqdm(
            stations.iterrows(),
            disable=not sys.stdout.isatty(),
            total=len(stations.index),
        )
        for iemid, row in progress:
            st4 = row["id"] if len(row["id"]) == 4 else f"K{row['id']}"
            url = (
                "https://aviationweather.gov/api/data/metar?"
                f"ids={st4}&format=raw&hours=36"
            )
            attempt = 0
            while attempt < 3:
                attempt += 1
                try:
                    resp = client.get(url, timeout=20)
                    if resp.status_code == 204:
                        if sys.stdout.isatty():
                            progress.write(f"No data for {st4}")
                        break
                    # 429 is a backoff warning
                    # 502 is a broken proxy service
                    if resp.status_code in [429, 502]:
                        LOG.info(
                            "Got %s, cooling jets for 5 seconds.",
                            resp.status_code,
                        )
                        time.sleep(5)
                        continue
                    if resp.status_code != 200:
                        LOG.warning("Failure %s %s", st4, resp.status_code)
                        continue
                    break
                except Exception as exp:
                    LOG.info("Failed to fetch %s: %s", st4, exp)
            awx = {}
            for line in resp.text.split("\n"):
                if line.strip() == "":
                    continue
                awx[line[5:11]] = f"{line}="
            res = conn.execute(
                sql_helper(
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

    # Insert into LDM for archival
    subprocess.call(
        [
            "pqinsert",
            "-p",
            f"data a {utc():%Y%m%d%H%M} text/supp_metars_via_avwxgov.txt "
            "text/supp_metars_via_avwxgov.txt txt",
            FN,
        ],
    )


if __name__ == "__main__":
    main()
