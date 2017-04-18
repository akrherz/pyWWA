from PIL import Image, ImageDraw, ImageFont
import numpy as np

TABLE = """127,14,123,-70
121,21,137,-69
107,26,139,-68
114,26,147,-67
93,37,156,-66
99,34,159,-65
76,49,172,-64
81,53,173,-63
78,58,183,-62
67,66,180,-61
63,71,193,-60
59,78,197,-59
51,82,210,-58
43,92,210,-57
40,101,227,-56
37,99,227,-55
32,110,232,-54
31,114,241,-53
26,121,243,-52
23,127,252,-51
23,130,252,-50
26,137,252,-49
31,143,252,-48
34,150,253,-47
40,156,253,-46
45,162,253,-45
49,168,253,-44
57,176,252,-43
64,182,253,-42
66,188,253,-41
71,196,253,-40
81,201,253,-39
83,206,254,-38
93,214,254,-37
98,223,250,-36
114,222,251,-35
111,234,254,-34
117,240,253,-33
125,246,255,-32
130,252,255,-31
133,255,254,-30
132,254,244,-29
134,255,255,-28
131,255,217,-27
134,255,196,-26
135,255,181,-25
133,253,164,-24
132,254,154,-23
131,254,138,-22
137,252,124,-21
133,254,134,-20
145,253,135,-19
159,254,136,-18
174,253,134,-17
188,254,132,-16
198,254,139,-15
212,254,135,-14
227,254,135,-13
239,254,138,-12
249,254,135,-11
255,254,136,-10
253,245,130,-9
243,244,125,-8
248,233,118,-7
255,226,103,-6
255,211,101,-5
252,211,88,-4
255,199,96,-3
254,193,76,-2
252,187,71,-1
254,176,73,0
254,169,64,1
254,163,57,2
252,154,50,3
254,149,45,4
251,143,40,5
254,132,38,6
242,114,34,7
233,108,31,8
237,103,30,9
227,102,29,10
226,96,28,11
221,89,30,12
218,86,26,13
207,84,27,14
207,76,24,15
199,74,22,16
192,67,21,17
187,63,20,18
180,61,19,19
178,57,18,20
174,48,17,21
168,41,17,22
164,37,18,23
156,34,13,24
155,28,16,25
142,27,11,26
139,22,10,27
139,17,13,28
126,12,8,29
131,4,10,30
129,3,8,31"""
# Generate a color ramp image, please


def main():
    """Do something"""
    font = ImageFont.truetype("/home/akrherz/projects/pyVBCam/lib/veramono.ttf", 10)

    rampin = np.zeros( (256,3), np.uint8)
    ramp = np.zeros( (256,3), np.uint8)
    data = np.zeros( (30,256), np.uint8)

    temps = []
    for i, line in enumerate(TABLE.split("\n")):
        tokens = line.split(",")
        rampin[i,0] = int(tokens[0])
        rampin[i,1] = int(tokens[1])
        rampin[i,2] = int(tokens[2])
        temps.append(float(tokens[3]))
    temps = np.array(temps)

    sattemps = []
    for i in range(256):
        if i > 176:
            val = (418-i) - 273
        else:
            val = (330. - (i/2.)) - 273.
        sattemps.append(val)
        if i < 255:
            data[0:15,i:i+1] = i
        if val < -81:
            ramp[i,:] = [0,0,0]
        elif val > max(temps):
            ramp[i,:] = [0,0,0]
        elif val < min(temps):
            ramp[i,:] = [255,255,255]
        else:
            idx = np.digitize([val,], temps)[0]
            ramp[i,:] = rampin[idx,:]

    o = open('gini_ir_ramp.txt', 'w')
    for i in range(256):
        o.write("%s %s %s\n" % (ramp[i,0], ramp[i,1], ramp[i,2]))
    o.close()

    ramp[255,:] = [255,255,255]

    png = Image.fromarray( np.fliplr(data) )
    png.putpalette( tuple(ramp.ravel()) )
    draw = ImageDraw.Draw(png)

    for i, temp in enumerate(sattemps):
        if temp % 20 != 0 or temp < -70:
            continue
        print temp % 20, temp
        lbl = "%.0f" % (temp,)
        (w,h) = font.getsize(lbl)
        draw.line( [255-i,17,255-i,10], fill=255)
        draw.text( (255-i-(w/2),18), lbl ,fill=255, font=font)

    draw.text( (235,18), 'C',fill=255, font=font)

    #draw.line( [0,0,255,0,255,29,0,29,0,0], fill=255)

    png.save("test.png")


if __name__ == '__main__':
    main()
