import xarray as xr
import fsspec
kfile = None
mapper = fsspec.get_mapper('reference://', fo=kfile, target_options={'compression':None})
ds = xr.open_zarr(mapper)