import os
import xarray as xr
import fsspec
import matplotlib.pyplot as plt
import json

import kerchunk_tools.parquet_refs as pqr

file = '/work/scratch-nopw/kerchunk/esacci_snow_swe_v2_5.json'
f = open(file,'r')
refs = json.load(f)
f.close()

#pqr.make_parquet_store('snow5_dap.parquet', refs)

mapper = fsspec.get_mapper("reference://", fo=file, target_options={"compression": None})
ds = xr.open_zarr(mapper)

#mapper = pqr.ParquetReferenceMapper('refs_test')
#fs = fsspec.filesystem('reference', fo=mapper, remote_protocol='file')
#ds = xr.open_zarr(fs.get_mapper(''))

ds.swe.mean(dim='time').plot()
plt.savefig('snow5_dap.png')
