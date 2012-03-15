from support import gini
import cStringIO
import sys
import Image
import mx.DateTime
import logging
import os
import tempfile
import random
import simplejson

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
        logger.info("ERROR: Unknown awips grid! |%s|" % (g.awips_grid(),))
        return
    

    
    tmpfn = tempfile.mktemp()
    png = Image.fromarray( g.data[:-1,:] )
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
    metadata = {'meta': {}}
    metadata['meta']['valid'] = g.metadata['valid'].strftime("%Y-%m-%dT%H:%M:%SZ")
    metadata['meta']['awips_grid'] = awips_grid
    metadata['meta']['archive_filename'] = archivefn
    metafp = '%s.json' % (tmpfn,)
    o = open(metafp, 'w')
    simplejson.dump(metadata, o)
    o.close()
    del(metadata['meta']['awips_grid'])
    metadata['meta']['epsg'] = 4326
    metafp = '%s_4326.json' % (tmpfn,)
    o = open(metafp, 'w')
    simplejson.dump(metadata, o)
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
    
    pqinsert = "/home/ldm/bin/pqinsert -p 'gis c %s gis/images/awips%s/%s GIS/sat/%s json' %s.json" % (
                                                g.metadata['valid'].strftime("%Y%m%d%H%M"), awips_grid,
                                                currentfn.replace("png", "json"), 
                                                archivefn.replace("png", "json"), tmpfn )
    if routes == 'ac':
        os.system(pqinsert)
        
    cmd = "gdalwarp -q -of GTiff -co 'WORLDFILE=ON' -s_srs '%s' -t_srs 'EPSG:4326' %s.png %s_4326.tif" % (
                                    g.metadata['proj'].srs, tmpfn, tmpfn)
    os.system(cmd)
    cmd = "convert %s_4326.tif %s_4326.png" %(tmpfn, tmpfn)
    os.system( cmd )
    # Need to randomize the .wld :(
    o = open('%s_4326.wld' % (tmpfn,), 'a')
    o.write( rand_zeros() )
    o.write( rand_zeros() )
    o.close()
    for suffix in ['wld', 'png', 'json']:
        pqinsert = "/home/ldm/bin/pqinsert -p 'gis c 000000000000 gis/images/4326/goes/%s bogus %s' %s_4326.%s" % (
                                                currentfn.replace('png', suffix), suffix, tmpfn, suffix)
        os.system(pqinsert)
        os.unlink("%s_4326.%s" % (tmpfn, suffix))
    
    os.unlink("%s.png" % (tmpfn,))
    os.unlink("%s.wld" % (tmpfn,))
    os.unlink("%s.json" % (tmpfn,))
    logger.info("Done!")

workflow()
