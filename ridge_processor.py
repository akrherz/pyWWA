"""
 I process activemq messages 10,000 at a time!
$Id: $:
"""

import Queue
import urllib2
import StringIO
import traceback
import urllib
import urllib2
import os
import random
import mx.DateTime
import pyactivemq
import simplejson
import pika

def jitter():
    return "0" * random.randint(0,6)

def generate_image(ch, method, properties, body):
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
    lts = mx.DateTime.DateTimeFromTicks( ticks / 1000. )
    gts = lts.gmtime()
    routes = "ac"
    if (mx.DateTime.gmt() - gts).seconds > 3600:
        routes = "a"

    metadata = {'meta': {}}
    metadata['meta']['valid'] = gts.strftime("%Y-%m-%dT%H:%M:%SZ")
    metadata['meta']['product'] = productID
    metadata['meta']['site'] = siteID
    metadata['meta']['vcp'] = vcp
    pqstr = "gis %s %s gis/images/4326/ridge/%s/%s_0.png GIS/ridge/%s/%s/%s_%s_%s.png png" % (
     routes, gts.strftime("%Y%m%d%H%M"), siteID, productID, 
     siteID, productID, siteID, productID, gts.strftime("%Y%m%d%H%M"))

    fp = '/tmp/%s_%s_%s.png' % (siteID, productID, 
        gts.strftime("%H%M"))
    o = open(fp, 'wb')
    o.write( body )
    o.close()

    metafp = '/tmp/%s_%s_%s.json' % (siteID, productID, gts.strftime("%H%M"))
    o = open(metafp, 'w')
    simplejson.dump(metadata, o)
    o.close()

    wldfp = '/tmp/%s_%s_%s.wld' % (siteID, productID, 
        gts.strftime("%H%M"))
    o = open(wldfp, 'w')
    o.write("%.6f%s\n" % ((lowerRightLon - upperLeftLon)/1000.0,jitter())) # dx
    o.write("0.0%s\n" % (jitter(),))
    o.write("0.0%s\n" % (jitter(),))
    o.write("%.6f%s\n" % (0 - (upperLeftLat - lowerRightLat)/1000.0,jitter())) # dy
    o.write("%.6f%s\n" % (upperLeftLon,jitter())) #UL Lon
    o.write("%.6f%s\n" % (upperLeftLat,jitter())) #UL Lat
    o.close()

    pqstr = "/home/ldm/bin/pqinsert -p '%s' %s" % (pqstr, fp)
    os.system( pqstr )
    os.system( pqstr.replace("png","wld") )
    metapq = pqstr.replace("png","json").replace(" ac ", " c ") 
    os.system( metapq )

    os.unlink(fp)
    os.unlink(wldfp)
    os.unlink(metafp)

def run():
    connection = pika.BlockingConnection(pika.ConnectionParameters(
        host='localhost'))

    channel = connection.channel()

    channel.exchange_declare(exchange='ridgeProductExchange', type='fanout', 
                             durable=True)
    result = channel.queue_declare(exclusive=True)
    queue_name = result.method.queue

    channel.queue_bind(exchange='ridgeProductExchange',
                   queue=queue_name)

    channel.basic_consume(generate_image,
                      queue=queue_name,
                      no_ack=True)

    channel.start_consuming()

if __name__ == '__main__':
    o = open("ridge_processor.pid",'w')
    o.write("%s" % ( os.getpid(),) )
    o.close()
    try:
        run()
    except KeyboardInterrupt:
        pass

