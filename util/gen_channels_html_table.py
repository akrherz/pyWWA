"""Utility script to generate the HTML used for IEMBot Channel Documentation"""

import os
import re
import sys

from pyiem.nws.nwsli import NWSLI
from pyiem.nws.products import parser as productparser
from pyiem.nws.products.cwa import parser as cwaparser
from pyiem.nws.products.dsm import parser as dsmparser
from pyiem.nws.products.vtec import parser as vtec_parser
from pyiem.nws.ugc import UGC
from pyiem.reference import prodDefinitions
from pyiem.util import logger

from pywwa.database import get_dbconnc

PARSERS = {
    "CWA": cwaparser,
    "DSM": dsmparser,
}
CHANNELSFN = "/home/akrherz/projects/iem/htdocs/projects/iembot/channels.html"
LOG = logger()
ugc_dict = {}
nwsli_dict = {}

C1 = "&lt;wfo&gt;"
C2 = "&lt;vtec_phenomena&gt;.&lt;vtec_significance&gt;"
C3 = "&lt;afos_pil&gt;"
C3p = "&lt;afos_pil_prefix&gt;..."
C35 = "&lt;afos_pil_first5&gt;..."
C3S = "&lt;afos_pil_first3&gt;.&lt;state&gt;"
C4 = "&lt;vtec_phenomena&gt;.&lt;vtec_significance&gt;.&lt;wfo&gt;"
C5 = "&lt;vtec_phenomena&gt;.&lt;vtec_significance&gt;.&lt;ugc&gt;"
C5s = "&lt;vtec_phenomena&gt;.&lt;vtec_significance&gt;.&lt;state&gt;"
C6 = "&lt;ugc&gt;"
C7 = "&lt;afos_pil&gt;.&lt;wfo&gt;"
C8 = "&lt;wmo_source&gt;.&lt;aaa&gt;"
C9 = "&lt;afos_pil&gt;.&lt;ugc&gt;"
D = {
    "10-313": "https://weather.gov/directives/sym/pd01003013curr.pdf",
    "10-314": "https://weather.gov/directives/sym/pd01003014curr.pdf",
    "10-315": "https://weather.gov/directives/sym/pd01003015curr.pdf",
    "10-320": "https://weather.gov/directives/sym/pd01003020curr.pdf",
    "10-330": "https://weather.gov/directives/sym/pd01003030curr.pdf",
    "10-401": "https://weather.gov/directives/sym/pd01004001curr.pdf",
    "10-511": "https://weather.gov/directives/sym/pd01005011curr.pdf",
    "10-512": "https://weather.gov/directives/sym/pd01005012curr.pdf",
    "10-513": "https://weather.gov/directives/sym/pd01005013curr.pdf",
    "10-515": "https://weather.gov/directives/sym/pd01005015curr.pdf",
    "10-517": "https://weather.gov/directives/sym/pd01005017curr.pdf",
    "10-601": "https://weather.gov/directives/sym/pd01006001curr.pdf",
    "10-803": "https://weather.gov/directives/sym/pd01008003curr.pdf",
    "10-912": "https://weather.gov/directives/sym/pd01009012curr.pdf",
    "10-922": "https://weather.gov/directives/sym/pd01009022curr.pdf",
    "10-930": "https://weather.gov/directives/sym/pd01009030curr.pdf",
    "10-1004": "https://weather.gov/directives/sym/pd01010004curr.pdf",
    "10-1701": "https://weather.gov/directives/sym/pd01017001curr.pdf",
}
SPECIAL = {
    "PTSDY1": "Storm Prediction Center Day 1 Convective Outlook",
    "PTSDY2": "Storm Prediction Center Day 2 Convective Outlook",
    "PFWFD1": "Storm Prediction Center Day 1 Fire Weather Outlook",
    "PFWFD2": "Storm Prediction Center Day 2 Fire Weather Outlook",
    "RBG94E": "Weather Prediction Center Day 1 Excessive Rainfall Outlook",
    "RBG98E": "Weather Prediction Center Day 2 Excessive Rainfall Outlook",
    "RBG99E": "Weather Prediction Center Day 3 Excessive Rainfall Outlook",
}

# TODO: TCV TSU ADR CDW DSA EQW HMW HPA LEw NUW RHW VOW PQS
# Our dictionary of products!
S1 = [C1, C2, C3, C3p, C4, C5, C5s, C6]
VTEC_PRODUCTS = [
    dict(afos="CFW", directive="10-320", channels=S1),
    dict(afos="EWW", directive="10-601", channels=S1),
    dict(afos="FFA", directive="10-922", channels=S1),
    dict(afos="FFS", directive="10-922", channels=S1),
    dict(afos="FFW", directive="10-922", channels=S1),
    dict(afos="FLS", directive="10-922", channels=S1),
    dict(afos="FLW", directive="10-922", channels=S1),
    dict(
        afos="MWW",
        directive="10-315",
        channels=[C2, C3, C3p, C4, C5, C6],
        notes=(
            "This product does not get routed to the "
            '<span class="badge bg-light text-dark">&lt;wfo&gt;</span> '
            "channel.  This is because the product is very frequently "
            "issued for offices with marine zones."
        ),
    ),
    dict(afos="NPW", directive="10-515", channels=S1),
    dict(afos="RFW", directive="10-401", channels=S1),
    dict(afos="SMW", directive="10-313", channels=S1),
    dict(afos="SQW", directive="10-511", channels=S1),
    dict(afos="SVR", directive="10-511", channels=S1),
    dict(afos="SVS", directive="10-511", channels=S1),
    dict(afos="TOR", directive="10-511", channels=S1),
    dict(afos="WCN", directive="10-511", channels=S1),
    dict(afos="WSW", directive="10-513", channels=S1),
]
S21 = [C3, C3p, C35]
S2 = [C3, C3p]
GEN_PRODUCTS = [
    dict(afos="ADA", directive="10-1701", channels=S2),
    dict(afos="ADM", directive="10-1701", channels=S2),
    dict(afos="AFD", directive="10-1701", channels=S2),
    dict(afos="AQI", directive="10-1701", channels=S2),
    dict(afos="AWU", directive="10-1701", channels=S2),
    dict(afos="AWW", directive="10-1701", channels=S2),
    dict(afos="AVA", directive="10-1701", channels=S2),
    dict(afos="AVG", directive="10-1701", channels=S2),
    dict(afos="AVW", directive="10-1701", channels=S2),
    dict(afos="AQA", directive="10-1701", channels=S2),
    dict(afos="BLU", directive="10-1701", channels=S2),
    dict(afos="CAE", directive="10-1701", channels=S2),
    dict(afos="CEM", directive="10-1701", channels=S2),
    dict(afos="CF6", directive="10-1004", channels=S2),
    dict(afos="CGR", directive="10-1701", channels=S2),
    dict(afos="CLI", directive="10-1004", channels=S2),
    dict(afos="CLM", directive="10-1004", channels=S2),
    dict(afos="CRF", directive="10-912", channels=S2),
    dict(afos="CWA", directive="10-803", channels=S2),
    dict(afos="CWF", directive="10-1701", channels=S2),
    dict(afos="DGT", directive="10-1701", channels=S2),
    dict(afos="DSM", directive="10-1701", channels=S2),
    dict(afos="ESF", directive="10-1701", channels=S2),
    dict(afos="EQI", directive="10-1701", channels=S2),
    dict(afos="EQR", directive="10-1701", channels=S2),
    dict(afos="EVI", directive="10-1701", channels=S2),
    dict(afos="FRW", directive="10-1701", channels=S2),
    dict(afos="FTM", directive="10-1701", channels=S2),
    dict(afos="FWA", directive="10-1701", channels=S2),
    dict(afos="FWF", directive="10-1701", channels=S2),
    dict(afos="FWS", directive="10-1701", channels=S2),
    dict(afos="GLF", directive="10-1701", channels=S2),
    dict(afos="HLS", directive="10-601", channels=S2),
    dict(afos="HCM", directive="10-1701", channels=S2),
    dict(afos="HMD", directive="10-1701", channels=S2),
    dict(afos="HWO", directive="10-517", channels=S2),
    dict(afos="HYD", directive="10-1701", channels=S2),
    dict(afos="ICE", directive="10-330", channels=S2),
    dict(afos="LAE", directive="10-1701", channels=S2),
    dict(afos="LCO", directive="10-1701", channels=S2),
    dict(
        afos="LSR",
        directive="10-517",
        channels=[
            "LSR.ALL",
            "LSR.&lt;typetext&gt;",
            "LSR.&lt;state&gt;",
            "LSR.&lt;state&gt;.&lt;typetext&gt;",
        ],
    ),
    dict(
        afos="MCD",
        directive="10-517",
        channels=[C3, C7],
        notes=(
            "The WFOs"
            " included are based on the ones highlighted by SPC within "
            "the text and not from a spatial check of their polygon."
        ),
    ),
    dict(
        afos="MPD",
        directive="10-517",
        channels=[C3, C7],
        notes=(
            "The WFOs"
            " included are based on the ones highlighted by WPC within "
            "the text and not from a spatial check of their polygon."
        ),
    ),
    dict(afos="MIS", directive="10-1701", channels=S2),
    dict(afos="MWS", directive="10-314", channels=S2),
    dict(afos="NOW", directive="10-517", channels=S2),
    dict(afos="NSH", directive="10-1701", channels=S2),
    dict(afos="OAV", directive="10-1701", channels=S2),
    dict(afos="OMR", directive="10-1701", channels=S2),
    dict(afos="PFM", directive="10-1701", channels=S2),
    dict(afos="PNS", directive="10-1701", channels=S2),
    dict(afos="PSH", directive="10-1701", channels=S2),
    dict(
        afos="PFWFD1",
        directive="10-512",
        channels=[
            C1,
            "&lt;wfo&gt;.SPCFD1",
            "&lt;wfo&gt;.SPCFD1.&lt;threshold&gt;",
        ],
        notes=("There is no present means for county/zone based channels."),
    ),
    dict(
        afos="PFWFD2",
        directive="10-512",
        channels=[
            C1,
            "&lt;wfo&gt;.SPCFD2",
            "&lt;wfo&gt;.SPCFD2.&lt;threshold&gt;",
        ],
        notes=("There is no present means for county/zone based channels."),
    ),
    dict(
        afos="PTSDY1",
        directive="10-512",
        channels=[
            C1,
            "&lt;wfo&gt;.SPCDY1",
            "&lt;wfo&gt;.SPCDY1.&lt;threshold&gt;",
        ],
        notes=("There is no present means for county/zone based channels."),
    ),
    dict(
        afos="PTSDY2",
        directive="10-512",
        channels=[
            C1,
            "&lt;wfo&gt;.SPCDY2",
            "&lt;wfo&gt;.SPCDY2.&lt;threshold&gt;",
        ],
        notes=("There is no present means for county/zone based channels."),
    ),
    dict(afos="REC", directive="10-1701", channels=S2),
    dict(afos="RER", directive="10-1004", channels=S2),
    dict(
        afos="RBG94E",
        directive="10-930",
        channels=[
            C1,
            "&lt;wfo&gt;.ERODY1",
            "&lt;wfo&gt;.ERODY1.&lt;threshold&gt;",
        ],
        notes=("There is no present means for county/zone based channels."),
    ),
    dict(
        afos="RBG98E",
        directive="10-930",
        channels=[
            C1,
            "&lt;wfo&gt;.ERODY2",
            "&lt;wfo&gt;.ERODY2.&lt;threshold&gt;",
        ],
        notes=("There is no present means for county/zone based channels."),
    ),
    dict(
        afos="RBG99E",
        directive="10-930",
        channels=[
            C1,
            "&lt;wfo&gt;.ERODY3",
            "&lt;wfo&gt;.ERODY3.&lt;threshold&gt;",
        ],
        notes=("There is no present means for county/zone based channels."),
    ),
    dict(afos="RRM", directive="10-1701", channels=S2),
    dict(afos="RFD", directive="10-1701", channels=S2),
    dict(afos="RTP", directive="10-1701", channels=S2),
    dict(afos="RVA", directive="10-1701", channels=S2),
    dict(afos="RVD", directive="10-922", channels=S2),
    dict(afos="RVF", directive="10-912", channels=S2),
    dict(afos="RWS", directive="10-1701", channels=S2),
    dict(afos="RVS", directive="10-1701", channels=S2),
    dict(afos="SAB", directive="10-1701", channels=S2),
    dict(afos="STF", directive="10-1701", channels=S2),
    dict(afos="SPS", directive="10-517", channels=[C3, C3p, C3S, C6, C9]),
    dict(afos="SRF", directive="10-1701", channels=S2),
    dict(afos="SPW", directive="10-1701", channels=S2),
    dict(afos="TAF", directive="10-1701", channels=[C3, C3p, C8]),
    dict(afos="TCD", directive="10-1701", channels=S21),
    dict(afos="TCM", directive="10-1701", channels=S21),
    dict(afos="TCP", directive="10-1701", channels=S21),
    dict(afos="TCU", directive="10-1701", channels=S21),
    dict(afos="TIB", directive="10-1701", channels=S2),
    dict(afos="TID", directive="10-320", channels=S2),
    dict(afos="TOE", directive="10-1701", channels=S2),
    dict(afos="TWO", directive="10-1701", channels=S21),
    dict(afos="WSV", directive="10-1701", channels=S2),
    dict(afos="VAA", directive="10-1701", channels=S2),
    dict(afos="WRK", directive="10-1701", channels=S2),
    dict(afos="ZFP", directive="10-1701", channels=S2),
]


def get_data(afos):
    """Return the text data for a given afos identifier"""
    fn = f"../examples/{afos}.txt"
    if not os.path.isfile(fn):
        print(f"File {fn} is missing")
    with open(fn, "rb") as fh:
        data = fh.read()
    return (
        data.decode("ascii")
        .replace("\r", "")
        .replace("\001\n", "")
        .replace("\003", "")
    )


def load_dicts():
    """Load up the directionaries"""
    pgconn, cursor = get_dbconnc("postgis")
    sql = """
        SELECT name, ugc, wfo from ugcs WHERE
        name IS NOT Null and end_ts is null
    """
    cursor.execute(sql)
    for row in cursor:
        nm = (row["name"]).replace("\x92", " ").replace("\xc2", " ")
        wfos = re.findall(r"([A-Z][A-Z][A-Z])", row["wfo"])
        ugc_dict[row["ugc"]] = UGC(
            row["ugc"][:2], row["ugc"][2], row["ugc"][3:], name=nm, wfos=wfos
        )

    sql = """SELECT nwsli,
     river_name || ' ' || proximity || ' ' || name || ' ['||state||']' as rname
     from hvtec_nwsli"""
    cursor.execute(sql)
    for row in cursor:
        nm = row["rname"].replace("&", " and ")
        nwsli_dict[row["nwsli"]] = NWSLI(row["nwsli"], name=nm)

    nwsli_dict["MLU"] = NWSLI("MLU", lat=32.52, lon=-92.03)
    nwsli_dict["IGB"] = NWSLI("MLU", lat=33.48, lon=-88.52)
    nwsli_dict["MEI"] = NWSLI("MLU", lat=32.38, lon=-88.80)
    pgconn.close()


def do_generic(fh):
    """Handle the generic case."""
    fh.write(
        """
    <h3>NWS Local Office / National Products</h3>
    <div class="table-responsive">
    <table class="table table-bordered table-sm">
    <thead class="table-light">
    <tr>
      <th scope="col" class="text-center"> </th>
      <th scope="col">AFOS PIL + Product Name</th>
      <th scope="col">Directive</th>
      <th scope="col">Channel Templates Used</th>
    </tr>
    </thead>
    """
    )
    for entry in GEN_PRODUCTS:
        afos: str = entry["afos"]
        if afos == "":
            continue
        try:
            v = PARSERS.get(afos, productparser)(
                get_data(afos),
                ugc_provider=ugc_dict,
                nwsli_provider=nwsli_dict,
            )
        except Exception as exp:
            LOG.info("ABORT: productparser %s failed", afos)
            LOG.exception(exp)
            sys.exit()
        if afos != "DSM":
            assert v.afos is not None
        j = v.get_jabbers("https://mesonet.agron.iastate.edu/p.php")
        channels = []
        _, html, xtra = j[0]
        tweet = xtra["twitter"] + "<br />"
        jmsg = html
        if isinstance(xtra["channels"], list):
            xtra["channels"] = ",".join(xtra["channels"])
        for channel in xtra["channels"].split(","):
            if channel not in channels:
                channels.append(channel)
        channels.sort()
        fh.write(
            """<tr>
  <td class="text-center align-middle">
    <button class="btn btn-sm" type="button"
      data-bs-toggle="collapse" data-bs-target="#channel_%s"
      aria-expanded="false">
      <i class="bi bi-plus-lg" aria-hidden="true"></i>
      <span class="visually-hidden">Toggle details</span>
    </button>
  </td>
  <td>%s (%s)</td>
  <td><a href="%s">%s</a></td>
  <td>%s</td>
</tr>
<tr>
  <td colspan="4">
    <div id="channel_%s" class="collapse">
    <dl class="row mb-0">
        %s
        <dt>Example Raw Text:</dt>
<dd><a href="https://mesonet.agron.iastate.edu/p.php?pid=%s">View Text</a></dd>
        <dt>Channels for Product Example:</dt><dd>%s</dd>
        <dt>XMPP Chatroom Example:</dt><dd>%s</dd>
        <dt>Twitter Example:</dt><dd>%s</dd>
        </dl>
        </div>
        </td>
        </tr>
        """
            % (
                afos,
                SPECIAL.get(afos, prodDefinitions.get(afos, afos)),
                afos,
                D[entry["directive"]],
                entry["directive"],
                " ".join(
                    [
                        f'<span class="badge bg-light text-dark">{s}</span>'
                        for s in entry["channels"]
                    ]
                ),
                afos,
                "<dt>Notes</dt><dd>%s</dd>" % (entry.get("notes"),)
                if "notes" in entry
                else "",
                v.get_product_id(),
                " ".join(
                    [
                        f'<span class="badge bg-light text-dark">{s}</span>'
                        for s in channels
                    ]
                ),
                jmsg,
                tweet,
            )
        )

    fh.write("""</table></div>""")


def do_vtec(fh):
    """Do VTEC"""
    fh.write(
        """
    <h3>NWS Products with P-VTEC and/or H-VTEC Included</h3>
    <div class="table-responsive">
    <table class="table table-bordered table-sm">
    <thead class="table-light">
    <tr>
      <th scope="col" class="text-center"> </th>
      <th scope="col">AFOS PIL + Product Name</th>
      <th scope="col">Directive</th>
      <th scope="col">Channel Templates Used</th>
    </tr>
    </thead>
    """
    )
    for entry in VTEC_PRODUCTS:
        afos = entry["afos"]
        v = vtec_parser(
            get_data(afos), ugc_provider=ugc_dict, nwsli_provider=nwsli_dict
        )
        if v.afos is None:
            LOG.info("afos was None for %s", entry)
            continue
        j = v.get_jabbers(
            "https://mesonet.agron.iastate.edu/vtec/",
            "https://mesonet.agron.iastate.edu/vtec/",
        )
        jmsg = ""
        tweet = ""
        channels = []
        for _, html, xtra in j:
            tweet += xtra["twitter"] + "<br />"
            jmsg += html
            for channel in xtra["channels"].split(","):
                if channel not in channels:
                    channels.append(channel)
        channels.sort()
        fh.write(
            """<tr>
  <td class="text-center align-middle">
    <button class="btn btn-sm" type="button"
      data-bs-toggle="collapse" data-bs-target="#channel_%s"
      aria-expanded="false">
      <i class="bi bi-plus-lg" aria-hidden="true"></i>
      <span class="visually-hidden">Toggle details</span>
    </button>
  </td>
  <td>%s (%s)</td>
  <td><a href="%s">%s</a></td>
  <td>%s</td>
</tr>
<tr>
  <td colspan="4">
    <div id="channel_%s" class="collapse">
    <dl class="row mb-0">
        %s
        <dt>Example Raw Text:</dt>
<dd><a href="https://mesonet.agron.iastate.edu/p.php?pid=%s">View Text</a></dd>
        <dt>Channels for Product Example:</dt><dd>%s</dd>
        <dt>XMPP Chatroom Example:</dt><dd>%s</dd>
        <dt>Twitter Example:</dt><dd>%s</dd>
        </dl>
        </div>
        </td>
        </tr>
        """
            % (
                afos,
                prodDefinitions.get(afos, afos),
                afos,
                D[entry["directive"]],
                entry["directive"],
                " ".join(
                    [
                        f'<span class="badge bg-light text-dark">{s}</span>'
                        for s in entry["channels"]
                    ]
                ),
                afos,
                "<dt>Notes</dt><dd>%s</dd>" % (entry.get("notes"),)
                if "notes" in entry
                else "",
                v.get_product_id(),
                " ".join(
                    [
                        f'<span class="badge bg-light text-dark">{s}</span>'
                        for s in channels
                    ]
                ),
                jmsg,
                tweet,
            )
        )

    fh.write("""</table></div>""")


def main():
    """Do Something Fun"""
    load_dicts()
    with open(CHANNELSFN, "w", encoding="utf-8") as fh:
        do_vtec(fh)
        do_generic(fh)


if __name__ == "__main__":
    # Go Main Go
    main()
