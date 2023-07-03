# Open paths
# Run kerchunk on each path

import os

f = open('filelists/cci_delivery/paths','r')
content = [c.replace('\n','') for c in f.readlines()]
f.close()

for x, line in enumerate(content):
    os.system(f'ls {line} > filelists/cci_delivery/esacci{x+2}.txt')
