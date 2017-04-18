"""Convert the FAA APT.txt file into a simple table file for ingestor"""

import sys


def convert_geo(val):
    """Convert 36-00-36.3550N to decimal """
    tokens = val.strip()[:-1].split("-")
    number = (((float(tokens[2]) / 60.) +
              float(tokens[1])) / 60. + float(tokens[0]))
    if val[-1] in ['W', 'S']:
        number = 0 - number
    return number


for line in open(sys.argv[1]):
    if line[:3] != 'APT':
        continue
    sid = line[27:31].strip()
    state = line[48:50]
    name = line[133:183].strip()
    lat = convert_geo(line[523:538])
    lon = convert_geo(line[550:565])

    print(('%-8s %6s %-32.32s %2s %2s %5i %6i %5s %2s'
           ) % (sid, '------', name, state, '--', lat * 100.,
                lon * 100., '--', '--'))
