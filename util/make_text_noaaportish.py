import sys
import os

fn = sys.argv[1]
fn2 = fn + ".tmp"

lines = open(fn).readlines()

out = open(fn2, 'w')

for i in range(len(lines)):
    line = lines[i]
    if i == 0 and lines[0] != '\001':
        out.write("\001\r\r\n")
    if line[-3:] != '\r\r\n':
        if line[-2:] == '\r\n':
            out.write(line[:-2] + "\r\r\n")
        else:
            out.write(line[:-1] + "\r\r\n")
    else:
        out.write(line)

out.write("\003")
out.close()
os.rename(fn2, fn)
