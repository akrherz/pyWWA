# Misc helpful stuff

import math

dirs = {'NNE': 22.5, 'ENE': 67.5, 'NE':  45.0, 'E': 90.0, 'ESE': 112.5,
        'SSE': 157.5, 'SE': 135.0, 'S': 180.0, 'SSW': 202.5,
        'WSW': 247.5, 'SW': 225.0, 'W': 270.0, 'WNW': 292.5,
        'NW': 315.0, 'NNW': 337.5, 'N': 0, '': 0}

KM_SM = 1.609347

def drct2dirTxt(dir):
  if (dir == None):
    return "N"
  dir = int(dir)
  if (dir >= 350 or dir < 13):
    return "N"
  elif (dir >= 13 and dir < 35):
    return "NNE"
  elif (dir >= 35 and dir < 57):
    return "NE"
  elif (dir >= 57 and dir < 80):
    return "ENE"
  elif (dir >= 80 and dir < 102):
    return "E"
  elif (dir >= 102 and dir < 127):
    return "ESE"
  elif (dir >= 127 and dir < 143):
    return "SE"
  elif (dir >= 143 and dir < 166):
    return "SSE"
  elif (dir >= 166 and dir < 190):
    return "S"
  elif (dir >= 190 and dir < 215):
    return "SSW"
  elif (dir >= 215 and dir < 237):
    return "SW"
  elif (dir >= 237 and dir < 260):
    return "WSW"
  elif (dir >= 260 and dir < 281):
    return "W"
  elif (dir >= 281 and dir < 304):
    return "WNW"
  elif (dir >= 304 and dir < 324):
    return "NW"
  elif (dir >= 324 and dir < 350):
    return "NNW"


def loc2lonlat(stationTable, site, direction, displacement):
    """
Compute the longitude and latitude of a point given by a site ID
and an offset ex) 2 SM NE of ALO
    """
    # Compute Base location
    lon0 = stationTable.sts[site]['lon']
    lat0 = stationTable.sts[site]['lat']
    x = -math.cos( math.radians( dirs[direction] + 90.0) )
    y = math.sin( math.radians( dirs[direction] + 90.0) )
    lat0 += (y * displacement * KM_SM / 111.11 )
    lon0 += (x * displacement * KM_SM /(111.11*math.cos( math.radians(lat0))))

    return lon0, lat0

def go2lonlat(lon0, lat0, direction, displacement):
    x = -math.cos( math.radians( dirs[direction] ) )
    y = math.sin( math.radians( dirs[direction] ) )
    lat0 += (y * displacement * KM_SM / 111.11 )
    lon0 += (x * displacement * KM_SM /(111.11*math.cos( math.radians(lat0))))

    return lon0, lat0

def drct2point(basex, basey, targetx, targety):
    if (basex == targetx):
        return 0
    slope = (targety - basey)/(targetx - basex)
    angle = math.fabs(math.atan(slope))
    # determine the sector :)
    if (targety >= basey and targetx > basex): # NE
        return  90 - math.degrees(angle)
    elif (targety < basey and targetx > basex): # SE
        return  90 + math.degrees(angle)
    elif (targety < basey and targetx < basex): # SW
        return  270 - math.degrees(angle)
    elif (targety > basey and targetx < basex): # NW
        return  270 + math.degrees(angle)
