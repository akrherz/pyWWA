from support import gini
import cStringIO
import sys
import Image
import mx.DateTime
import logging
import os
import tempfile
import random

FORMAT = "%(asctime)-15s:["+ str(os.getpid()) +"]: %(message)s"
LOG_FN = 'logs/gini2gis-%s.log' % (mx.DateTime.gmt().strftime("%Y%m%d"),)
logging.basicConfig(filename=LOG_FN, filemode='a+', format=FORMAT)
logger=logging.getLogger()
logger.addHandler(logging.StreamHandler())
logger.setLevel(logging.INFO)

def rand_zeros():
    return "%s" % ("0" * random.randint(0,10),) 

def workflow():
    logger.info("Starting Ingest for: %s" % (" ".join(sys.argv),))
    c = cStringIO.StringIO()
    c.write( sys.stdin.read() )
    c.seek(0)
    g = gini.GINIZFile( c )
    #
    logger.info( str(g) )
    archivefn = g.archive_filename()
    logger.info("Processed archive file: "+ archivefn)
    currentfn = g.current_filename()
    awips_grid = g.awips_grid()
    if awips_grid is None:
        logger.info("ERROR: Unknown awips grid!")
        return
    tmpfn = tempfile.mktemp()
    
    png = Image.fromarray( g.data )
    png.save('%s.png' % (tmpfn,))
    # World File
    o = open('%s.wld' % (tmpfn,), 'w')
    fmt = """%(dx).3f"""+ rand_zeros() +"""
0.0"""+ rand_zeros() +"""
0.0"""+ rand_zeros() +"""
-%(dy).3f"""+ rand_zeros() +"""
%(x0).3f"""+ rand_zeros() +"""
%(y1).3f"""
    o.write( fmt % g.metadata)
    o.close()
    # Metadata
    o = open("%s.txt" % (tmpfn,), 'w')
    o.write("""
    http://www.nws.noaa.gov/noaaport/html/icdtb48e.html
    AWIPS Grid: %s
    
    Archive Filename: %s
    Valid: %s 
    
    Contact Info: Daryl. Herzmann akrherz@iastate.edu 515 294 5978
    """ % (awips_grid, archivefn, g.metadata['valid']))
    o.close()

    routes = "ac"
    if (mx.DateTime.gmt() - g.metadata['valid']).minutes > 120:
        routes = "a"

    pqinsert = "/home/ldm/bin/pqinsert -p 'gis %s %s gis/images/awips%s/%s GIS/sat/%s png' %s.png" % (
                                                routes, g.metadata['valid'].strftime("%Y%m%d%H%M"), awips_grid,
                                                currentfn, archivefn, tmpfn )
    os.system(pqinsert)
    pqinsert = "/home/ldm/bin/pqinsert -p 'gis %s %s gis/images/awips%s/%s GIS/sat/%s wld' %s.wld" % (
                                                routes, g.metadata['valid'].strftime("%Y%m%d%H%M"), awips_grid,
                                                currentfn.replace("png", "wld"), 
                                                archivefn.replace("png", "wld"), tmpfn )
    os.system(pqinsert)
    
    pqinsert = "/home/ldm/bin/pqinsert -p 'gis c %s gis/images/awips%s/%s GIS/sat/%s txt' %s.txt" % (
                                                g.metadata['valid'].strftime("%Y%m%d%H%M"), awips_grid,
                                                currentfn.replace("png", "txt"), 
                                                archivefn.replace("png", "txt"), tmpfn )
    if routes == 'ac':
        os.system(pqinsert)
    os.unlink("%s.png" % (tmpfn,))
    os.unlink("%s.wld" % (tmpfn,))
    os.unlink("%s.txt" % (tmpfn,))
    logger.info("Done!")

workflow()
