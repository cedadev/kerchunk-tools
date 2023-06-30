from netCDF4 import Dataset

fname = '/neodc/esacci/land_surface_temperature/data/AQUA_MODIS/L3C/0.01/v3.00/daily/2002/07/04/ESACCI-LST-L3C-LST-MODISA-0.01deg_1DAILY_NIGHT-20020704000000-fv3.00.nc'

test = Dataset(fname)
dtime = test.variables['dtime']
print(list(test.variables.keys()))
#print(dir(dtime))

#for var in test.variables.keys():
#    if len(test.variables[var].chunking()) 
chunkdims = []
for dim in test.dimensions.keys():
    chunkdims.append(test.dimensions[dim].size)

print(test.dimensions['lat'].size)

chunkdimsizes = []
for dim in dtime.chunking():
    if dim > 1:
        chunkdimsizes.append(dim)


# Get chunking for a single variable


# Obtain a list of variables
# Determine the ones with the most number of dimensions