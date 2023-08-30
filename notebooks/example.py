import fsspec
import xarray as xr
import matplotlib.pyplot as plt
import sys
import math
import numpy as np


DIR = '/home/users/dwest77/Documents/kerchunk_dev/public/kerchunk-tools/notebooks/'

def find_variable(ds, display=False):
    keepvar = None
    for var in ds.variables:
        if display:
            print(var, len(ds[var].dims))
        if not keepvar and len(ds[var].dims) == 3:
            keepvar = var
    if keepvar == None:
        if not display:
            find_variable(ds, display=True)
        else:
            return None
    return keepvar

def expected(value, plevel):
    if value >= 1000:
        return expected(value/1000, plevel+1)
    else:
        return plevel

pref = ['B','KB','MB','GB','TB']

def getbox(map_bounds, latn, lonn):
    latmin = int((map_bounds[2]+90)/180 * latn)
    latmax = int((map_bounds[0]+90)/180 * latn)
    
    lonmin = int((map_bounds[3]+180)/360 * lonn)
    lonmax = int((map_bounds[1]+180)/360 * lonn)
    return [latmin, latmax, lonmin, lonmax]

#@profile
def main(x):
    if '.py' in x:
        x = '2'
    kfile = f'https://dap.ceda.ac.uk/neodc/esacci/snow/docs/esacci{x}.json'
    #kfile = 'atsr_test.json'
    #kfile = f'/neodc/esacci/snow/docs/esacci{x}.json'

    mapper = fsspec.get_mapper('reference://', fo=kfile) # Add remote options for auth-required files.
    ds = xr.open_zarr(mapper, consolidated=False)
    latn = len(ds.lat)
    lonn = len(ds.lon)
    timen = len(ds.time)

    # Get base box size (very small)
    map_bounds = [52, -1, 51, -2]

    [latmin, latmax, lonmin, lonmax] = getbox(map_bounds, latn, lonn)

    bytes = (latmax-latmin)*(lonmax-lonmin)*64*timen
    
    ref = 1e9

    sf = math.sqrt(ref/bytes)
    print(sf)
    plevel = expected(bytes, 0)
    print(bytes/(1000**plevel), pref[plevel],'basic')
    if sf < 75:
        map_bounds = [
            52 + (sf-1)/2,
            -1 + (sf-1)/2,
            51 - (sf-1)/2,
            -2 - (sf-1)/2
        ]

        [latmin, latmax, lonmin, lonmax] = getbox(map_bounds, latn, lonn)
        bytes = (latmax-latmin)*(lonmax-lonmin)*64*timen
    else:
        map_bounds = [90, 180, -90, -180]
        latmin = 0
        latmax = latn
        lonmin = 0
        lonmax = lonn
        bytes = (latmax-latmin)*(lonmax-lonmin)*64*timen

    plevel = expected(bytes, 0)
    print(bytes/(1000**plevel), pref[plevel],'expected')
    print('New map bounds', map_bounds)

    vname = find_variable(ds)
    print('Computing for ',vname)
    if vname != None:
        var = ds[vname]
        print(latmin, latmax, lonmin, lonmax, timen, vname)
        timei = -1
        filled = False
        while not filled and timei < timen-1:
            timei += 1
            print(f'{timei} {np.count_nonzero(np.isnan(var[timei,latmin:latmax,lonmin:lonmax]))*100 / (bytes/(64*timen)):.3f}% NaN')
            if np.count_nonzero(np.isnan(var[timei,latmin:latmax,lonmin:lonmax]))/(bytes/(64*timen)) < 1:
                filled = True

        if filled:                                                                       
            var = var[timei,latmin:latmax,lonmin:lonmax]

            var.plot()
            plt.savefig(f'{vname}.png')
            print(f'Plotted {vname}.png')
        else:
            print('No non-empty time slices found')

main(sys.argv[-1])