"""
 Something to store UGC information!
"""

import re

#_re = "([A-Z][A-Z][C,Z][0-9][0-9][0-9][A-Z,0-9,\-,>]+)"
_re = "(([A-Z]?[A-Z]?[C,Z]?[0-9]{3}[>\-])+)([0-9]{6})-"

class ugc:

    def __init__(self, raw):
        self.raw = raw.replace("\n", "").replace(" ","")
        self.ugc = []
        self.rawexpire = None
        self.findUGC()
       

    def findUGC(self):
        tokens = re.findall(_re, self.raw)
        if (len(tokens) == 0):
            return
        parts = re.split('-', tokens[0][0])
        self.rawexpire = tokens[0][2]
        stateCode = ""
        for i in range(len(parts) ):
            if (i == 0):
                ugcType = parts[0][2]
            thisPart = parts[i]
            if len(thisPart) == 6: # We have a new state ID
                stateCode = thisPart[:3]
                self.ugc.append(thisPart)
            elif len(thisPart) == 3: # We have an individual Section
                self.ugc.append(stateCode+thisPart)
            elif len(thisPart) > 6: # We must have a > in there somewhere
                newParts = re.split('>', thisPart)
                firstPart = newParts[0]
                secondPart = newParts[1]
                if len(firstPart) > 3:
                    stateCode = firstPart[:3]
                firstVal = int( firstPart[-3:] )
                lastVal = int( secondPart )
                if (ugcType == "C"):
                    for j in range(0, lastVal+2 - firstVal, 2):
                        strCode = "000"+str(firstVal+j)
                        self.ugc.append(stateCode+strCode[-3:])
                else:
                    for j in range(firstVal, lastVal+1):
                        strCode = "000"+str(j)
                        self.ugc.append(stateCode+strCode[-3:])

#u = ugc("DCZ001-MDZ004>007-009>011-013-014-016>018-VAZ036>042-050>057-170200-/")
#u = ugc("ALC001-005>011-015>021-027-029-037-047-051-055-057-063-065-073-075-081-085>093-101-105>127-271200-")

#print u.raw
#print u.ugc

