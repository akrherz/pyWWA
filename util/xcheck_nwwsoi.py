"""Review our NWWS-OI file for deltas."""
# stdlib
import sys

# third party
import requests
from pyiem.nws.product import TextProduct


def main(argv):
    """Go Main Go."""
    fn = argv[1]
    with open(fn, 'rb') as fh:
        for token in fh.read().decode("ascii", "ignore").split("\003"):
            if len(token) < 10:
                continue
            prod = TextProduct(token)
            if prod.afos is None:
                continue
            pid = prod.get_product_id()
            uri = f"https://mesonet.agron.iastate.edu/api/1/nwstext/{pid}"
            req = requests.get(uri)
            if req.status_code != 200:
                print(uri)


if __name__ == "__main__":
    main(sys.argv)
