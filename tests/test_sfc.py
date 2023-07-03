## Open file list
## Kerchunk each file separately
## Compare kerchunk sizes with netcdf file sizes

import os
import json
import numpy as np
import matplotlib.pyplot as plt

from kerchunk.hdf import SingleHdf5ToZarr
from kerchunk.netCDF3 import NetCDF3ToZarr

def getfilelist():
    f = open('testcci.txt','r')
    content = f.readlines()
    f.close()
    return [c.replace('\n','') for c in content]

def count_refs(nfile):
    try:
        print('Using HDF5 reader')
        tdict = SingleHdf5ToZarr(nfile).translate()
        return len(json.dumps(tdict['refs']))/1000
    except OSError:
        print('Switching to NetCDF3 reader')
        try:
            tdict = NetCDF3ToZarr(nfile).translate()
            return len(json.dumps(tdict['refs']))/1000
        except:
            return False
        return False

def get_filesize(nfile):
    return os.stat(nfile).st_size

def savedata(lists):
    word = ''
    for l in lists:
        word += ','.join(l) + '\n'
    f = open('ccidata','w')
    f.write(word)
    f.close()

def main():
    flist = getfilelist()
    netcdfs = []
    kerchunks = []
    k1 = count_refs(flist[0])
    for f in flist:
        nc = get_filesize(f)
        netcdfs.append(nc)

    k_avg = k1 * (np.mean(netcdfs)/netcdfs[0])
    print(netcdfs[0], np.mean(netcdfs))
    ktotal = k_avg * len(flist)
    print(f'Old: {k1*len(flist)/1:.3f} KB - New: {ktotal/1:.3f} KB')

#main()
count_refs('/neodc/esacci/cloud/data/version3/L3C/AVHRR-PM/v3.0/AVHRR_NOAA_14/2001/200104-ESACCI-L3C_CLOUD-CLD_PRODUCTS-AVHRR_NOAA-14-fv3.0.nc')