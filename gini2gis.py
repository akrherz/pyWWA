from support import gini
import cStringIO
import sys
import Image
import mx.DateTime
import logging
import os
import tempfile

FORMAT = "%(asctime)-15s:: %(message)s"
logging.basicConfig(filename='logs/gini2gis.log', filemode='a+', format=FORMAT)
logger=logging.getLogger()
logger.addHandler(logging.StreamHandler())
logger.setLevel(logging.INFO)

def workflow():
    logger.info("Starting Ingest")
    c = cStringIO.StringIO()
    c.write( sys.stdin.read() )
    c.seek(0)
    g = gini.GINIZFile( c )
    #
    print str(g)
    archivefn = g.archive_filename()
    logger.info("Processed archive file: "+ archivefn)
    currentfn = g.current_filename()
    tmpfn = tempfile.mktemp()
    
    png = Image.fromarray( g.data )
    png.save('%s.png' % (tmpfn,))
    # World File
    o = open('%s.wld' % (tmpfn,), 'w')
    o.write("""%(dx).3f
0.000000000000%(random).0f
0.0
-%(dy).3f
%(x0).3f
%(y1).3f""" % g.metadata)
    o.close()
    # Metadata
    o = open("%s.txt" % (tmpfn,), 'w')
    o.write("""
    http://www.nws.noaa.gov/noaaport/html/icdtb48e.html
    Grid Info:  Lambert Comic Comformal lat_0=25n lon_0=95w
    
    Archive Filename: %s
    Valid: %s 
    
    Contact Info: Daryl. Herzmann akrherz@iastate.edu 515 294 5978
    """ % (archivefn, g.metadata['valid']))
    o.close()


    pqinsert = "/home/ldm/bin/pqinsert -p 'gis ac %s gis/images/awips211/%s GIS/sat/%s png' %s.png" % (g.metadata['valid'].strftime("%Y%m%d%H%M"),
                                                currentfn, archivefn, tmpfn )
    os.system(pqinsert)
    pqinsert = "/home/ldm/bin/pqinsert -p 'gis ac %s gis/images/awips211/%s GIS/sat/%s wld' %s.wld" % (g.metadata['valid'].strftime("%Y%m%d%H%M"),
                                                currentfn.replace("png", "wld"), 
                                                archivefn.replace("png", "wld"), tmpfn )
    os.system(pqinsert)
    pqinsert = "/home/ldm/bin/pqinsert -p 'gis c %s gis/images/awips211/%s GIS/sat/%s txt' %s.txt" % (g.metadata['valid'].strftime("%Y%m%d%H%M"),
                                                currentfn.replace("png", "txt"), 
                                                archivefn.replace("png", "txt"), tmpfn )
    os.system(pqinsert)
    os.unlink("%s.png" % (tmpfn,))
    os.unlink("%s.wld" % (tmpfn,))
    os.unlink("%s.txt" % (tmpfn,))
    logger.info("Done!")
workflow()
