"""
I convert raw GINI noaaport imagery into geo-referenced PNG files both in the
'native' projection and 4326.  

Questions? daryl herzmann akrherz@iastate.edu
"""

# https://github.com/akrherz/pyIEM
from pyiem.nws import gini
import pytz
import cStringIO
import sys
import Image
import datetime
import logging
import os
import tempfile
import random
import json
import subprocess

FORMAT = "%(asctime)-15s:["+ str(os.getpid()) +"]: %(message)s"
LOG_FN = 'logs/gini2gis-%s.log' % (datetime.datetime.utcnow().strftime("%Y%m%d"),)
logging.basicConfig(filename=LOG_FN, filemode='a+', format=FORMAT)
logger = logging.getLogger()
logger.addHandler(logging.StreamHandler())
logger.setLevel(logging.INFO)

PQINSERT = "/home/ldm/bin/pqinsert"

def rand_zeros():
    """
    Generate a random number of zeros
    """
    return "%s" % ("0" * random.randint(0, 10),) 

def process_input():
    """
    Process what was provided to use by LDM on stdin
    @return GINIZFile instance
    """
    cstr = cStringIO.StringIO()
    cstr.write( sys.stdin.read() )
    cstr.seek(0)
    sat = gini.GINIZFile( cstr )
    logger.info(str(sat))
    return sat

def write_gispng(sat, tmpfn):
    """
    Write PNG file with associated .wld file.
    """
    png = Image.fromarray( sat.data[:-1,:] )
    png.save('%s.png' % (tmpfn,))
    # World File
    out = open('%s.wld' % (tmpfn,), 'w')
    fmt = """%(dx).3f"""+ rand_zeros() +"""
0.0"""+ rand_zeros() +"""
0.0"""+ rand_zeros() +"""
-%(dy).3f"""+ rand_zeros() +"""
%(x0).3f"""+ rand_zeros() +"""
%(y1).3f"""
    out.write( fmt % sat.metadata)
    out.close()
    
    cmd = "%s -p 'gis %s %s gis/images/awips%s/%s GIS/sat/awips%s/%s png' %s.png" % (
                        PQINSERT, get_ldm_routes(sat), 
                        sat.metadata['valid'].strftime("%Y%m%d%H%M"), 
                        sat.awips_grid(), sat.current_filename(), 
                        sat.awips_grid(), sat.archive_filename(), tmpfn )
    subprocess.call( cmd, shell=True )

    cmd = "%s -p 'gis %s %s gis/images/awips%s/%s GIS/sat/awips%s/%s wld' %s.wld" % (
                        PQINSERT, get_ldm_routes(sat), 
                        sat.metadata['valid'].strftime("%Y%m%d%H%M"), sat.awips_grid(),
                        sat.current_filename().replace("png", "wld"), sat.awips_grid(),
                        sat.archive_filename().replace("png", "wld"), tmpfn )
    subprocess.call( cmd, shell=True )

def write_metadata(sat, tmpfn):
    """
    Write a JSON formatted metadata file
    """
    metadata = {'meta': {}}
    metadata['meta']['valid'] = sat.metadata['valid'].strftime("%Y-%m-%dT%H:%M:%SZ")
    metadata['meta']['awips_grid'] = sat.awips_grid()
    metadata['meta']['bird'] = sat.get_bird()
    metadata['meta']['archive_filename'] = sat.archive_filename()
    metafp = '%s.json' % (tmpfn,)
    out = open(metafp, 'w')
    json.dump(metadata, out)
    out.close()
    
    cmd = "%s -p 'gis c %s gis/images/awips%s/%s GIS/sat/%s json' %s.json" % (
                            PQINSERT,
                            sat.metadata['valid'].strftime("%Y%m%d%H%M"), 
                            sat.awips_grid(),
                            sat.current_filename().replace("png", "json"), 
                            sat.archive_filename().replace("png", "json"), 
                            tmpfn )
    subprocess.call( cmd, shell=True )
    os.unlink("%s.json" % (tmpfn,))

def write_metadata_epsg(sat, tmpfn, epsg):
    """
    Write a JSON formatted metadata file
    """
    metadata = {'meta': {}}
    metadata['meta']['valid'] = sat.metadata['valid'].strftime("%Y-%m-%dT%H:%M:%SZ")
    metadata['meta']['epsg'] = epsg
    metadata['meta']['bird'] = sat.get_bird()
    metadata['meta']['archive_filename'] = sat.archive_filename()
    metafp = '%s_%s.json' % (tmpfn, epsg)
    out = open(metafp, 'w')
    json.dump(metadata, out)
    out.close()
    
    cmd = "%s -p 'gis c 000000000000 gis/images/%s/goes/%s bogus json' %s_%s.json" % (
                            PQINSERT, epsg,
                            sat.current_filename().replace("png", "json"), 
                            tmpfn, epsg)
    subprocess.call( cmd, shell=True )
    os.unlink("%s_%s.json" % (tmpfn, epsg))

def get_ldm_routes(sat):
    """
    Figure out if this product should be routed to current or archived folders
    """
    utcnow = datetime.datetime.utcnow().replace(tzinfo=pytz.timezone("UTC"))
    minutes = (utcnow - sat.metadata['valid']).seconds / 60.0
    if minutes > 120:
        return "a"
    return "ac"

def gdalwarp(sat, tmpfn, epsg):
    """
    Convert imagery into some EPSG projection, typically 4326
    """
    cmd = "gdalwarp -q -of GTiff -co 'TFW=YES' -s_srs '%s' -t_srs 'EPSG:%s' %s.png %s_%s.tif" % (
                                    sat.metadata['proj'].srs, epsg, 
                                    tmpfn, tmpfn, epsg)
    subprocess.call( cmd, shell=True )

    # Convert file back to PNG for use and archival (smaller file)
    cmd = "convert -quiet %s_%s.tif %s_4326.png" % (tmpfn, epsg, tmpfn)
    proc = subprocess.Popen( cmd, shell=True, stderr=subprocess.PIPE,
                             stdout=subprocess.PIPE )
    output = proc.stderr.read()
    if output != "":
        logger.error("gdalwarp() convert error message: %s" % (output,))
    os.unlink("%s_%s.tif" % (tmpfn, epsg))

    out = open('%s_%s.tfw' % (tmpfn, epsg), 'a')
    out.write( rand_zeros() )
    out.write( rand_zeros() )
    out.close()
    
    cmd = "%s -p 'gis c 000000000000 gis/images/%s/goes/%s bogus wld' %s_%s.tfw" % (
             PQINSERT, epsg, sat.current_filename().replace('png', 'wld'), 
             tmpfn, epsg)
    subprocess.call( cmd, shell=True )
    cmd = "%s -p 'gis c 000000000000 gis/images/%s/goes/%s bogus png' %s_%s.png" % (
             PQINSERT, epsg, sat.current_filename(), tmpfn, epsg)
    subprocess.call( cmd, shell=True )
    os.unlink("%s_%s.png" % (tmpfn, epsg))
    os.unlink("%s_%s.tfw" % (tmpfn, epsg))


def cleanup(tmpfn):
    """
    Pickup after ourself 
    """
    for suffix in ['png', 'wld', 'tfw']:
        if os.path.isfile("%s.%s" % (tmpfn, suffix)):
            os.unlink("%s.%s" % (tmpfn, suffix))

def workflow():
    logger.info("Starting Ingest for: %s" % (" ".join(sys.argv),))
    
    sat = process_input()
    logger.info("Processed archive file: "+ sat.archive_filename())
    if  sat.awips_grid() is None:
        logger.info("ABORT: Unknown awips grid!")
        return
    
    # Generate a temporary filename to use for our work
    tmpfn = tempfile.mktemp()
    # Write PNG
    write_gispng(sat, tmpfn)
    
    if get_ldm_routes(sat) == 'ac':
        # Write JSON metadata
        write_metadata(sat, tmpfn)
        # Write JSON metadata for 4326 file
        write_metadata_epsg(sat, tmpfn, 4326)
        # Warp the file into 4326
        gdalwarp(sat, tmpfn, 4326)    
    # cleanup after ourself
    cleanup(tmpfn)

    logger.info("Done!")

workflow()
