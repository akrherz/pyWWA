import struct
import math
import pyproj
import numpy as np
import zlib
import mx.DateTime
import random
import logging

M_PI_2 = 1.57079632679489661923
M_PI = 3.14159265358979323846
RE_METERS = 6371200.0
ENTITIES = ['','','','','','','','DMSP','GMS','METEOSAT','GOES7', 'GOES8',
            'GOES9', 'GOES10', 'GOES11', 'GOES12', 'GOES13', 'GOES14', 'GOES15']
CHANNELS = ['','VIS','3.9', 'WV', 'IR', '12', '13.3', 'U7', 'U8', 'U9', 'U10']
SECTORS = ['NHCOMP', 'EAST', 'WEST', 'AK', 'AKNAT', 'HI', 'HINAT', 'PR', 'PRNAT','SUPER']

def uint24(data):
    u = int(struct.unpack('>B', data[0] )[0]) << 16
    u += int(struct.unpack('>B', data[1] )[0]) << 8
    u += int(struct.unpack('>B', data[2] )[0])
    return u

def int24(data):
    u = int(struct.unpack('>B', data[0] )[0] & 127) << 16
    u += int(struct.unpack('>B', data[1] )[0]) << 8
    u += int(struct.unpack('>B', data[2] )[0])
    if (struct.unpack('>B', data[0] )[0] & 128) != 0:
       u *= -1
    return u


class GINIFile(object):
    
    def __init__(self, fobj):

        self.metadata = self.read_header(hdata)
    
    def __str__(self):
        s = "Line Size: %s Num Lines: %s" % (self.metadata['linesize'],
                                             self.metadata['numlines'])
        return s
    
    def current_filename(self):
        """
        Return a filename for this product, we'll use the format
        {SOURCE}_{SECTOR}_{CHANNEL}_{VALID}.png
        """
        return "%s_%s_%s.png" % (ENTITIES[self.metadata['creating_entity']],
                                    SECTORS[self.metadata['sector']],
                                    CHANNELS[self.metadata['channel']])
    def archive_filename(self):
        """
        Return a filename for this product, we'll use the format
        {SOURCE}_{SECTOR}_{CHANNEL}_{VALID}.png
        """
        return "%s_%s_%s_%s.png" % (ENTITIES[self.metadata['creating_entity']],
                                    SECTORS[self.metadata['sector']],
                                    CHANNELS[self.metadata['channel']],
                                    self.metadata['valid'].strftime("%Y%m%d%H%M"))
    def init_llc(self):
        """
        Initialize Lambert Conic Comformal
        """
        self.metadata['proj'] = pyproj.Proj(proj='lcc', lat_0=self.metadata['latin'],
         lat_1=self.metadata['latin'], lat_2=self.metadata['latin'], lon_0=self.metadata['lov'],
         a=6371200.0,b=6371200.0)

        s = 1.0
        if self.metadata['proj_center_flag'] != 0:
            s = -1.0
        psi = M_PI_2 - abs( math.radians( self.metadata['latin'] ))
        cos_psi = math.cos(psi)
        r_E = RE_METERS / cos_psi
        alpha = math.pow(math.tan(psi/2.0), cos_psi) / math.sin(psi)
    
        x0, y0 = self.metadata['proj'](self.metadata['lon1'], self.metadata['lat1'])
        self.metadata['x0'] = x0
        self.metadata['y0'] = y0
        #self.metadata['dx'] *= alpha
        #self.metadata['dy'] *= alpha
        # TODO: Somehow, I am off :(
        self.metadata['y1'] = y0 + ( self.metadata['dy'] * self.metadata['ny'])

        self.metadata['lon_ul'], self.metadata['lat_ul'] =  self.metadata['proj'](self.metadata['x0'],
                                                    self.metadata['y1'], inverse=True)
        logging.info("lat1: %.5f y0: %5.f y1: %.5f lat: %.5f alpha: %.5f dy: %.3f" % (
                    self.metadata['lat1'], y0, self.metadata['y1'], 
                    self.metadata['lat_ul'], alpha, self.metadata['dy']))



    def init_projection(self):
        """
        Setup Grid and projection details
        """
        if self.metadata['map_projection'] == 3:
            self.init_llc()

    def read_header(self, hdata):
        meta = {}
        meta['source'] = struct.unpack("> B", hdata[0] )[0]
        meta['creating_entity'] = struct.unpack("> B", hdata[1] )[0]
        meta['sector'] = struct.unpack("> B", hdata[2] )[0]
        meta['channel'] = struct.unpack("> B", hdata[3] )[0]
    
        meta['numlines'] = struct.unpack(">H", hdata[4:6] )[0]
        meta['linesize'] = struct.unpack(">H", hdata[6:8] )[0]
    
        yr = 1900 + struct.unpack("> B", hdata[8] )[0]
        mo = struct.unpack("> B", hdata[9] )[0]
        dy = struct.unpack("> B", hdata[10] )[0]
        hh = struct.unpack("> B", hdata[11] )[0]
        mi = struct.unpack("> B", hdata[12] )[0]
        ss = struct.unpack("> B", hdata[13] )[0]
        hs = struct.unpack("> B", hdata[14] )[0]
        meta['valid'] = mx.DateTime.DateTime(yr,mo,dy,hh,mi,ss)
        meta['random'] = int( meta['valid'] ) * random.random()
        meta['map_projection'] = struct.unpack("> B", hdata[15] )[0]
        meta['proj_center_flag'] = (struct.unpack("> B", hdata[36] )[0] >> 7)
        meta['scan_mode'] = struct.unpack("> B", hdata[37] )[0]
    
        meta['nx'] = struct.unpack(">H", hdata[16:18] )[0]
        meta['ny'] = struct.unpack(">H", hdata[18:20] )[0]
        meta['res'] = struct.unpack(">B", hdata[41] )[0]
        # Mercator
        if meta['map_projection'] == 1:
            meta['lat1'] = int24( hdata[20:23] )
            meta['lon1'] = int24( hdata[23:26] )
            meta['lov'] = 0
            meta['dx'] = struct.unpack(">H", hdata[33:35] )[0]
            meta['dy'] = struct.unpack(">H", hdata[35:37] )[0]
            meta['latin'] = int24( hdata[38:41] )
            meta['lat2'] = int24( hdata[27:30] )
            meta['lon2'] = int24( hdata[30:33] )
            meta['lat_ur'] = int24( hdata[55:58] )
            meta['lon_ur'] = int24( hdata[58:61] )
        else:
            meta['lat1'] = int24( hdata[20:23] )
            meta['lon1'] = int24( hdata[23:26] )
            meta['lov'] = int24( hdata[27:30] )
            meta['dx'] = uint24( hdata[30:33] )
            meta['dy'] = uint24( hdata[33:36] )
            meta['latin'] = int24( hdata[38:41] )
            meta['lat2'] = 0
            meta['lon2'] = 0
            meta['lat_ur'] = int24( hdata[55:58] )
            meta['lon_ur'] = int24( hdata[58:61] )
    
        meta['dx'] = meta['dx'] / 10.0
        meta['dy'] = meta['dy'] / 10.0
        meta['lat1'] = meta['lat1'] / 10000.0
        meta['lon1'] = meta['lon1'] / 10000.0
        meta['lov'] = meta['lov'] / 10000.0
        meta['latin'] = meta['latin'] / 10000.0
        meta['lat2'] = meta['lat2'] / 10000.0
        meta['lon2'] = meta['lon2'] / 10000.0
        meta['lat_ur'] = meta['lat_ur'] / 10000.0
        meta['lon_ur'] = meta['lon_ur'] / 10000.0
    
        return meta

"""
Deal with compressed GINI files, which are the standard on NOAAPORT
"""
class GINIZFile(GINIFile):
    
    def __init__(self, fobj):
        fobj.seek(0)
        # WMO HEADER
        fobj.read(21)
        d = zlib.decompressobj()
        hdata = d.decompress(fobj.read())
        self.metadata = self.read_header(hdata[21:])
        self.init_projection()
        loc = 0
        sdata = ""
        for i in range(0,len(d.unused_data)-1):
            a = struct.unpack("> B", d.unused_data[i] )[0]
            b = struct.unpack("> B", d.unused_data[i+1] )[0]
            if a == 120 and b == 218:
                if loc > 0:
                    #print 'sz', i - loc, i, len(n)
                    try:
                        sdata += zlib.decompress(d.unused_data[loc:i])
                        #print 'Chunk', loc, i, np.shape(np.fromstring( zlib.decompress(d.unused_data[loc:]), np.int8 ))
                    except:
                        #print 'ERROR, keep going', i
                        pass
                loc = i
        #try:
        sdata += zlib.decompress(d.unused_data[loc:])
        #except:
        #    sdata += d.unused_data[loc:]
        data = np.array( np.fromstring( sdata , np.int8) )
        pad = self.metadata['linesize'] * self.metadata['numlines'] - np.shape(data)[0]
        if pad > 0:
            fewer = pad / self.metadata['linesize']
            #data = np.append(data, np.zeros( (pad), np.int8))
            self.metadata['numlines'] -= fewer
            self.metadata['ny'] -= fewer
        self.data = np.reshape(data, (self.metadata['numlines'], self.metadata['linesize']))
        # Erm, this bothers me, but need to redo, if ny changed!
        self.init_projection()
