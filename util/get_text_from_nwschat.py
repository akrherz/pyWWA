"""Hit the NWSChat quasi web service to get the text IEM may have missed :("""

import sys
import tempfile

import requests


def wrap(data):
    """convert data into more noaaportish"""
    data = data.replace("&gt;", ">").replace("&lt;", "<")
    return "\001" + data.replace("&amp;", "&").replace("\n", "\r\r\n") + "\003"


def process(j):
    """Process the json data j"""
    if not j["data"]:
        print("ERROR: No results found!")
        return

    with tempfile.NamedTemporaryFile(mode="w", delete=False) as tmpfd:
        tmpfd.write(wrap(j["data"][0]["report"]))
        for svs in j["data"][0]["svs"]:
            tmpfd.write(wrap(svs))
        print(f"Created {tmpfd.name}")


def main(argv):
    """Do Work please"""
    year = argv[1]
    wfo = argv[2]
    phenomena = argv[3]
    significance = argv[4]
    eventid = argv[5]
    uri = (
        "https://nwschat.weather.gov/vtec/json-text.php?"
        "year=%s&wfo=%s&phenomena=%s&eventid=%s&significance=%s"
    ) % (year, wfo, phenomena, eventid, significance)

    req = requests.get(uri)
    process(req.json())


if __name__ == "__main__":
    main(sys.argv)
