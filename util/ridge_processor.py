"""I process activemq messages 10,000 at a time!"""
import os
import subprocess
import datetime
import json

import pytz
import pika


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
    gts = gts.replace(tzinfo=pytz.utc)
    utcnow = datetime.datetime.utcnow().replace(tzinfo=pytz.UTC)
    routes = "ac"
    if (utcnow - gts).seconds > 3600:
        routes = "a"

    metadata = {"meta": {}}
    metadata["meta"]["valid"] = gts.strftime("%Y-%m-%dT%H:%M:%SZ")
    metadata["meta"]["product"] = productID
    metadata["meta"]["site"] = siteID
    metadata["meta"]["vcp"] = vcp
    pqstr = (
        "gis %s %s gis/images/4326/ridge/%s/%s_0.png "
        "GIS/ridge/%s/%s/%s_%s_%s.png png"
    ) % (
        routes,
        gts.strftime("%Y%m%d%H%M"),
        siteID,
        productID,
        siteID,
        productID,
        siteID,
        productID,
        gts.strftime("%Y%m%d%H%M"),
    )

    pngfn = "/tmp/%s_%s_%s.png" % (siteID, productID, gts.strftime("%H%M"))
    with open(pngfn, "wb") as fh:
        fh.write(body)

    metafn = "/tmp/%s_%s_%s.json" % (siteID, productID, gts.strftime("%H%M"))
    with open(metafn, "w") as fh:
        json.dump(metadata, fh)

    wldfn = "/tmp/%s_%s_%s.wld" % (siteID, productID, gts.strftime("%H%M"))
    with open(wldfn, "w") as fh:
        fh.write("%.6f\n" % ((lowerRightLon - upperLeftLon) / 1000.0,))  # dx
        fh.write("0.0\n")
        fh.write("0.0\n")
        fh.write(
            "%.6f\n" % (0 - (upperLeftLat - lowerRightLat) / 1000.0,)
        )  # dy
        fh.write("%.6f\n" % (upperLeftLon,))  # UL Lon
        fh.write("%.6f\n" % (upperLeftLat,))  # UL Lat

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
            print(
                "Strange file: %s was missing, but should be deleted" % (fn,)
            )


def run():
    """Go run Go."""
    with open("ridge_processor.pid", "w") as fh:
        fh.write("%s" % (os.getpid(),))

    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host="iem-rabbitmq.local")
    )
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
