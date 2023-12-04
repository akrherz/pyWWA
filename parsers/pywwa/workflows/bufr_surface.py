"""Ingest BUFR (^IS WMO Header) Data.

This is a work in progress yet, notes and todo as follows:

- [ ] Generate a METAR with a IEM_BUFR flag aset
- Issuer of Identifier (20000+ is WMO program), less is local
- 20006 indicates ICAO would be the last ID

"""
# stdlib
import warnings
from datetime import timedelta

# 3rd Party
from pybufrkit.decoder import Decoder, generate_bufr_message
from pybufrkit.renderer import NestedJsonRenderer
from pyiem.nws.product import TextProduct
from pyiem.observation import Observation
from pyiem.util import LOG, convert_value, utc
from twisted.internet import reactor
from twisted.internet.task import LoopingCall

# Local
from pywwa import common
from pywwa.database import get_database
from pywwa.ldm import bridge

# pdbufr running in flat mode
warnings.filterwarnings("ignore", module="pdbufr")
MESOSITEDB = get_database("mesosite", cp_max=1)
IEMDB = get_database("iem", cp_max=1)
# Github Copilot did 99% of the work here, hopefully it is right!
WMO2ISO3166 = {
    "BABJ": 156,  # China
    "BIRK": 352,  # Iceland
    "CWAO": 124,  # Canada
    "DAMM": 682,  # Saudi Arabia
    "DEMS": 276,  # Germany
    "DKPY": 208,  # Denmark
    "DRRN": 562,  # Niger
    "DXXX": 768,  # Senegal
    "EBUM": 56,  # Belgium
    "EBWM": 56,  # Belgium
    "EDZW": 276,  # Germany
    "EEMH": 233,  # Estonia
    "EFKL": 246,  # Finland
    "EGGR": 826,  # United Kingdom
    "EGRR": 826,  # United Kingdom
    "EHDB": 528,  # Netherlands
    "EIDB": 372,  # Ireland
    "EKMI": 208,  # Denmark
    "ELLX": 442,  # Luxembourg
    "ENMI": 578,  # Norway
    "ESWI": 752,  # Sweden
    "EUMS": 578,  # Norway
    "EYHM": 233,  # Estonia
    "FAPR": 710,  # South Africa
    "FQMA": 508,  # Mozambique
    "FMEE": 262,  # Reunion
    "FNLU": 24,  # Angola
    "FTTJ": 120,  # Cameroon
    "FWCL": 120,  # Cameroon
    "GMAA": 504,  # Morocco
    "GMAG": 504,  # Morocco
    "GMDA": 504,  # Morocco
    "GMFB": 504,  # Morocco
    "GMFM": 504,  # Morocco
    "GMFC": 504,  # Morocco
    "GMFF": 504,  # Morocco
    "GMFI": 504,  # Morocco
    "GMFK": 504,  # Morocco
    "GMFO": 504,  # Morocco
    "GMMC": 504,  # Morocco
    "GMME": 504,  # Morocco
    "GMMG": 504,  # Morocco
    "GMMI": 504,  # Morocco
    "GMMK": 504,  # Morocco
    "GMMN": 504,  # Morocco
    "GMMO": 504,  # Morocco
    "GMMP": 504,  # Morocco
    "GMMS": 504,  # Morocco
    "GMMW": 504,  # Morocco
    "GMMX": 504,  # Morocco
    "GMMZ": 504,  # Morocco
    "GMSE": 504,  # Morocco
    "GMTA": 504,  # Morocco
    "GMTI": 504,  # Morocco
    "GMTL": 504,  # Morocco
    "GMTN": 504,  # Morocco
    "GMTT": 504,  # Morocco
    "GOOY": 686,  # Senegal
    "GVAC": 132,  # Cape Verde
    "HABP": 356,  # India
    "HECA": 818,  # Egypt
    "HKNC": 404,  # Kenya
    "KNES": 840,  # United States
    "KWNB": 840,  # United States
    "LCLK": 196,  # Cyprus
    "LDZM": 705,  # Slovenia
    "LEMM": 724,  # Spain
    "LFPW": 250,  # France
    "LGAT": 300,  # Greece
    "LIIB": 380,  # Italy
    "LLBD": 376,  # Israel
    "LJLM": 705,  # Slovenia
    "LOWM": 40,  # Austria
    "LPMG": 620,  # Portugal
    "LQSM": 760,  # Syria
    "LSSW": 756,  # Switzerland
    "LTAA": 792,  # Turkey
    "LYBM": 688,  # Montenegro
    "LYPG": 688,  # Montenegro
    "LZIB": 703,  # Slovak Republic
    "MJSK": 484,  # Mexico
    "NCRG": 554,  # New Zealand
    "NFFN": 242,  # Fiji
    "NFTF": 548,  # New Caledonia
    "NLWW": 548,  # New Caledonia
    "NTAA": 258,  # French Polynesia
    "NWBB": 548,  # New Caledonia
    "NVVV": 548,  # New Caledonia
    "NZKL": 554,  # New Zealand
    "OEJD": 682,  # Saudi Arabia
    "OKPR": 203,  # Czech Republic
    "OPKC": 586,  # Pakistan
    "PANC": 840,  # United States
    "RKSL": 410,  # South Korea
    "RJTD": 392,  # Japan
    "RUHB": 643,  # Russia
    "RUML": 643,  # Russia
    "RUMS": 643,  # Russia
    "RUNW": 643,  # Russia
    "SABM": 32,  # Argentina
    "SAWB": 32,  # Argentina
    "SCSC": 152,  # Chile
    "SGAS": 858,  # Uruguay
    "SKBO": 170,  # Colombia
    "SBBR": 76,  # Brazil
    "SEQU": 218,  # Ecuador
    "SLLP": 68,  # Bolivia
    "SOCA": 74,  # French Guiana
    "SOWR": 724,  # Spain
    "SPIM": 604,  # Peru
    "SUMU": 858,  # Uruguay
    "TBPB": 52,  # Barbados
    "TLPL": 662,  # Saint Lucia
    "UAST": 398,  # Kazakhstan
    "UKMS": 804,  # Ukraine
    "UMMN": 643,  # Russia
    "UMRR": 643,  # Russia
    "UTTW": 860,  # Uzbekistan
    "VBRR": 764,  # Thailand
    "VDPP": 764,  # Thailand
    "VGDC": 826,  # United Kingdom
    "VMMC": 344,  # Hong Kong
    "VRMM": 462,  # Maldives
    "VTBB": 764,  # Thailand
    "WIIX": 360,  # Indonesia
    "WSSS": 702,  # Singapore
    "YRBK": 36,  # Australia
    "ZATI": 156,  # China
}
UNKNOWNS = []
WIGOS = {}
NETWORK = "WMO_BUFR_SRF"
DIRECTS = {
    "004001": "year",
    "004002": "month",
    "004003": "day",
    "004004": "hour",
    "004005": "minute",
    "001015": "station_name",
    "007030": "elevation",
    "005001": "lat",
    "006001": "lon",
}


def bounds_check(val, low, high):
    """Ensure some QC."""
    if val is None or val < low or val > high:
        return None
    return val


def load_xref(txn):
    """Build out WIGOS2IEMID."""
    txn.execute(
        "SELECT wigos, iemid, tzname from stations "
        "WHERE wigos is not null and network = %s",
        (NETWORK,),
    )
    for row in txn.fetchall():
        WIGOS[row["wigos"]] = {
            "iemid": row["iemid"],
            "tzname": row["tzname"],
        }
    LOG.info("Loaded %s WIGOS2IEMID entries", len(WIGOS))


def add_station(txn, sid, data) -> int:
    """Add a mesosite station entry."""
    if "lon" not in data or "lat" not in data:
        LOG.info("Skipping %s as no location data", sid)
        WIGOS[sid] = {"iemid": -2}
        return None
    sname = data.get("sname")
    if sname is None:
        LOG.info("Skipping %s as no station name %s", sid, data)
        WIGOS[sid] = {"iemid": -2}
        return None
    sname = sname.replace(",", " ").replace("\x00", "")
    elev = data.get("elevation")
    txn.execute(
        """
        INSERT into stations(id, wigos, name, network, online,
        geom, elevation, metasite, plot_name, country)
        VALUES (%s, %s, %s, %s, 't',
        ST_Point(%s, %s, 4326), %s, 'f', %s, 'UN') returning iemid
        """,
        (
            sid,
            sid,
            sname,
            NETWORK,
            data["lon"],
            data["lat"],
            elev,
            sname,
        ),
    )
    iemid = txn.fetchone()["iemid"]
    WIGOS[sid] = {"iemid": iemid, "tzname": None}
    return iemid


def process_messages(txn, prod, msgs):
    """Do what we can do."""
    data = glean_data(msgs, prod.source)
    if not data:
        return
    sid = data.get("sid")
    if sid is None:
        return
    # Remove null bytes
    data["sid"] = sid.replace("\x00", "")
    valid = data["valid"]
    # Don't allow products from the future
    if valid > (common.utcnow() + timedelta(hours=2)):
        LOG.warning(
            "%s %s is from the future %s > %s",
            prod.get_product_id(),
            sid,
            valid,
            common.utcnow() + timedelta(hours=2),
        )
        return
    meta = WIGOS.get(sid, {"iemid": None})
    if meta["iemid"] is None:
        # prevent race condition
        WIGOS[sid] = {"iemid": -1}
        meta = WIGOS[sid]
        df = MESOSITEDB.runInteraction(add_station, sid, data)
        msg = f"{sid} {prod.get_product_id()}, {data}"
        df.addErrback(common.email_error, msg)
    if meta["iemid"] < -1:
        # add station failed, so do nothing
        return
    if meta["iemid"] == -1:
        LOG.info("Skipping %s as iemid is currently -1", sid)
        return
    LOG.debug("%s %s %s %s", sid, meta["iemid"], prod.get_product_id(), data)
    # This likely means that IEM station metadata has yet to sync, so there
    # is no iemaccess entry
    if meta["tzname"] is None:
        LOG.info("Skipping %s as tzname is None", sid)
        return
    ob = Observation(valid=valid, iemid=meta["iemid"], tzname=meta["tzname"])
    ob.data.update(data)
    ob.data["raw"] = f"BUFR: {prod.get_product_id()}"
    ob.save(txn)


def render_members(members, msgs):
    """recursive."""
    for member in members:
        if isinstance(member, list):
            render_members(member, msgs)
        elif isinstance(member, dict):
            if "factor" in member:
                LOG.debug("FACTOR: %s", member["factor"])
                msgs.append(member["factor"])
            if "value" in member:
                if member["value"] is not None:
                    LOG.debug(member)
                    msgs.append(member)
            elif "members" in member:
                render_members(member["members"], msgs)
            else:
                LOG.debug("Dead end", member)
        else:
            LOG.debug(member)
            msgs.append(member)


def glean_data(msgs, source):
    """see what we can do with this."""
    data = {}
    displacement = 0
    for msg in msgs:
        LOG.debug("%s %s %s", msg["id"], msg["description"], msg["value"])
        if msg["id"].startswith("001"):
            data[msg["id"]] = msg["value"]
            continue
        if msg["id"] == "004024":  # TIME PERIOD OR DISPLACEMENT
            displacement = msg["value"]
            continue
        if msg["id"] in DIRECTS:
            data[DIRECTS[msg["id"]]] = msg["value"]
            continue
        if msg["id"] == "010004":
            data["pres"] = msg["value"] / 100.0
            continue
        if msg["id"] == "010051":
            data["mslp"] = msg["value"] / 100.0

            continue
        if msg["id"] == "012101":
            data["tmpf"] = bounds_check(
                convert_value(msg["value"], "degK", "degF"),
                -100,
                150,
            )
            continue
        if msg["id"] == "012103":
            data["dwpf"] = bounds_check(
                convert_value(msg["value"], "degK", "degF"),
                -100,
                150,
            )
            continue
        if msg["id"] == "013003":
            data["relh"] = bounds_check(
                msg["value"],
                0,
                100,
            )
            continue
        if msg["id"] == "020001":
            data["vsby"] = bounds_check(
                convert_value(msg["value"], "m", "mile"), 0, 100
            )
            continue
        if msg["id"] == "011002" and displacement >= -10:
            data["sknt"] = bounds_check(
                convert_value(msg["value"], "meter per second", "knot"),
                0,
                200,
            )
            continue

        if msg["id"] == "011001" and displacement >= -10:
            data["drct"] = bounds_check(msg["value"], 0, 360)
            continue
        if msg["id"] == "011041" and displacement >= -10:
            data["gust"] = bounds_check(
                convert_value(msg["value"], "meter per second", "knot"), 0, 200
            )
            continue
        if msg["id"] == "011043" and displacement >= -10:
            data["gust_drct"] = bounds_check(msg["value"], 0, 360)
            continue
    if "year" not in data:
        return {}
    try:
        data["valid"] = utc(
            data["year"],
            data["month"],
            data["day"],
            data.get("hour", 0),  # Unsure if this is too forgiving
            data.get("minute", 0),
        )
    except ValueError as exp:
        LOG.info("ValueError in utc(): %s %s", data, exp)
        return {}
    # Attempt to compute a station ID
    if "001125" in data:
        data["sid"] = (
            f"{data['001125']}-"
            f"{data['001126']}-"
            f"{data['001127']}-"
            f"{data['001128'].decode('ascii', 'ignore').strip()}"
        )
    if "001002" in data:
        ccode = WMO2ISO3166.get(source)
        if ccode is None:
            if source not in UNKNOWNS:
                UNKNOWNS.append(source)
                raise ValueError(f"Unknown WMO2ISO3166 {source}")
            return {}
        data["sid"] = f"0-{ccode}-0-{data['001002']}"
    if "001015" in data:
        data["sname"] = data["001015"].decode("ascii", "ignore").strip()
    return data


def processor(txn, prod, prodbytes) -> int:
    """Protect the realprocessor"""
    # ATTM we are sending this to the general text parser, so to set
    # various needed attributes.
    try:
        msgs = []
        for bufr_message in generate_bufr_message(Decoder(), prodbytes):
            msgs.append(bufr_message)
    except Exception as exp:
        LOG.info(
            "%s %s %s",
            exp,
            prod.get_product_id(),
            prod.source,
        )
        return 0
    total = 0
    for bufmsg in msgs:
        jdata = NestedJsonRenderer().render(bufmsg)
        for section in jdata:
            for parameter in section:
                if isinstance(parameter["value"], list):
                    LOG.debug(
                        "--> %s %s",
                        parameter["name"],
                        type(parameter["value"]),
                    )
                    for entry in parameter["value"]:
                        LOG.debug("----> entry")
                        if isinstance(entry, list):
                            msgs = []
                            render_members(entry, msgs)
                            total += len(msgs)
                            process_messages(txn, prod, msgs)
                        else:
                            # unexpanded_descriptors
                            LOG.debug(entry)
                else:
                    LOG.debug(
                        "--> %s %s", parameter["name"], parameter["value"]
                    )
    return total


def ingest(prodbytes):
    """Gets called by the LDM Bridge."""
    pos = prodbytes.find(b"BUFR")
    if pos == -1:
        if prodbytes != b"" and prodbytes.find(b"NIL") == -1:
            LOG.warning("No BUFR found in %s", prodbytes[:100])
        return None, None
    header = prodbytes[:pos].decode("ascii")
    prod = TextProduct(header, utcnow=common.utcnow(), parse_segments=False)
    meat = prodbytes[pos:]
    defer = IEMDB.runInteraction(processor, prod, meat)
    defer.addErrback(common.email_error, prod.get_product_id())
    defer.addErrback(LOG.warning)
    return prod, meat


def ready(_):
    """Callback once we are ready."""
    bridge(ingest, isbinary=True)
    lc = LoopingCall(MESOSITEDB.runInteraction, load_xref)
    df = lc.start(10800, now=False)
    df.addErrback(common.email_error)


def main():
    """Go Main Go."""
    common.main(with_jabber=False)
    df = MESOSITEDB.runInteraction(load_xref)
    df.addCallback(ready)
    df.addErrback(common.email_error)
    reactor.run()


if __name__ == "__main__":
    # Do Stuff
    main()
