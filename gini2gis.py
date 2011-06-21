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
    tmpfn = tempfile.mktemp()
    
    png = Image.fromarray( g.data )
    png.save('%s.png' % (tmpfn,))
    
    o = open('%s.wld' % (tmpfn,), 'w')
    o.write("""%(dx).3f
0.000000000000%(random).0f
0.0
-%(dy).3f
%(x0).3f
%(y1).3f""" % g.metadata)
    o.close()

    archivefn = g.archive_filename()
    logger.info("Processed archive file: "+ archivefn)
    currentfn = g.current_filename()
    pqinsert = "/home/ldm/bin/pqinsert -p 'gis ac %s %s GIS/sat/%s png' %s.png" % (g.metadata['valid'].strftime("%Y%m%d%H%M"),
                                                currentfn, archivefn, tmpfn )
    os.system(pqinsert)
    pqinsert = "/home/ldm/bin/pqinsert -p 'gis ac %s %s GIS/sat/%s wld' %s.wld" % (g.metadata['valid'].strftime("%Y%m%d%H%M"),
                                                currentfn.replace("png", "wld"), 
                                                archivefn.replace("png", "wld"), tmpfn )
    os.system(pqinsert)
    
    os.unlink("%s.png" % (tmpfn,))
    os.unlink("%s.wld" % (tmpfn,))
    logger.info("Done!")
workflow()