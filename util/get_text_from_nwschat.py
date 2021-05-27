"""Hit the NWSChat quasi web service to get the text IEM may have missed :("""
from __future__ import print_function
import sys

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

    out = open("/tmp/vtec_data.txt", "w")
    out.write(wrap(j["data"][0]["report"]))
    for svs in j["data"][0]["svs"]:
        out.write(wrap(svs))

    out.close()


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
