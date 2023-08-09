import xarray as xr
import numpy as np

file = '/neodc/esacci/biomass/data/agb/maps/v4.0/netcdf/ESACCI-BIOMASS-L4-AGB-MERGED-100m-2010-fv4.0.nc'

ds = xr.open_mfdataset(file)

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
vname = find_variable(ds)
print(vname)
var = ds[vname]
print(np.count_nonzero(np.isnan(var)))