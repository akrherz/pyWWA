"""Process ridge exchange messages that contain imagery."""

import datetime
import json
import os
import subprocess
from zoneinfo import ZoneInfo

import pika


def get_rabbitmqconn():
    """Load the configuration."""
    fn = os.sep.join([os.path.dirname(__file__), "rabbitmq.json"])
    with open(fn, "r", encoding="utf-8") as fh:
        config = json.load(fh)
    return pika.BlockingConnection(
        pika.ConnectionParameters(
            host=config["host"],
            port=config["port"],
            virtual_host=config["vhost"],
            credentials=pika.credentials.PlainCredentials(
                config["user"], config["password"]
            ),
        )
    )


def generate_image(_ch, _method, properties, body):
    """
    Generate a Lite Image given the ActiveMQ message
    @param pyactivemq.BytesMessage
    """
    siteID = properties.headers["siteID"]
    ticks = properties.headers["validTime"]
    productID = properties.headers["productID"]
    vcp = properties.headers["vcp"]
    upperLeftLat = float(properties.headers["upperLeftLat"])
    upperLeftLon = float(properties.headers["upperLeftLon"])
    lowerRightLat = float(properties.headers["lowerRightLat"])
    lowerRightLon = float(properties.headers["lowerRightLon"])
    # Convert Java ticks into local time
    gts = datetime.datetime(1970, 1, 1) + datetime.timedelta(
        seconds=(ticks / 1000)
    )
    gts = gts.replace(tzinfo=ZoneInfo("UTC"))
    utcnow = datetime.datetime.utcnow().replace(tzinfo=ZoneInfo("UTC"))
    routes = "ac"
    if (utcnow - gts).seconds > 3600:
        routes = "a"

    metadata = {"meta": {}}
    metadata["meta"]["valid"] = gts.strftime("%Y-%m-%dT%H:%M:%SZ")
    metadata["meta"]["product"] = productID
    metadata["meta"]["site"] = siteID
    metadata["meta"]["vcp"] = vcp
    pqstr = (
        f"gis {routes} {gts:%Y%m%d%H%M} "
        f"gis/images/4326/ridge/{siteID}/{productID}_0.png "
        f"GIS/ridge/{siteID}/{productID}/{siteID}_{productID}_"
        f"{gts:%Y%m%d%H%M}.png png"
    )

    pngfn = f"/tmp/{siteID}_{productID}_{gts:%H%M}.png"
    with open(pngfn, "wb") as fh:
        fh.write(body)

    metafn = f"/tmp/{siteID}_{productID}_{gts:%H%M}.json"
    with open(metafn, "w", encoding="ascii") as fh:
        json.dump(metadata, fh)

    wldfn = f"/tmp/{siteID}_{productID}_{gts:%H%M}.wld"
    with open(wldfn, "w", encoding="ascii") as fh:
        _val = (lowerRightLon - upperLeftLon) / 1000.0
        fh.write(f"{_val:.6f}\n")  # dx
        fh.write("0.0\n")
        fh.write("0.0\n")
        _val = 0 - (upperLeftLat - lowerRightLat) / 1000.0
        fh.write(f"{_val:.6f}\n")  # dy
        fh.write(f"{upperLeftLon:.6f}\n")  # UL Lon
        fh.write(f"{upperLeftLat:.6f}\n")  # UL Lat

    # Use -i to allow for duplicate file content as the product id *should*
    # always be unique
    pqstr = f"pqinsert -i -p '{pqstr}' {pngfn}"
    subprocess.call(pqstr, shell=True)
    subprocess.call(pqstr.replace("png", "wld"), shell=True)
    metapq = pqstr.replace("png", "json").replace(" ac ", " c ")
    subprocess.call(metapq, shell=True)

    for fn in [pngfn, wldfn, metafn]:
        if os.path.isfile(fn):
            os.unlink(fn)
        else:
            print(f"Strange file: {fn} was missing, but should be deleted")


def run():
    """Go run Go."""
    with open("ridge_processor.pid", "w", encoding="ascii") as fh:
        fh.write(f"{os.getpid()}")

    connection = get_rabbitmqconn()
    channel = connection.channel()

    channel.exchange_declare(
        "ridgeProductExchange", exchange_type="fanout", durable=True
    )
    result = channel.queue_declare("ridgeProductExchange", exclusive=True)
    queue_name = result.method.queue

    channel.queue_bind(exchange="ridgeProductExchange", queue=queue_name)

    channel.basic_consume(queue_name, generate_image, auto_ack=True)

    channel.start_consuming()


if __name__ == "__main__":
    run()
