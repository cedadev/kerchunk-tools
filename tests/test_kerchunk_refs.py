import os
import json
import numpy as np
import matplotlib.pyplot as plt

from kerchunk.hdf import SingleHdf5ToZarr
from kerchunk.netCDF3 import NetCDF3ToZarr

def getkerchunk(kfile):
    f = open(kfile,'r')
    content = json.load(f)
    f.close()
    return content

content = getkerchunk('/home/users/dwest77/Documents/kerchunk_dev/kerchunk-tools/kerchunk_tools/kc-indexes/ccis/cci8a.json')

non_refs = len(json.dumps(content)) - len(json.dumps(content['refs']))
refs_json = content['refs']
refsizes = []
for part in refs_json.keys():
    refsizes.append(str(len(json.dumps(refs_json[part]))))
print(f'{ np.mean(np.array(refsizes,dtype=int)) :.3f} avg ref, {len(refsizes)} total refs' )
f = open('result','w')
f.write('\n'.join(refsizes))
f.close()



