import os
import importlib

import xarray as xr
import fsspec
import s3fs

try:
    from set_configs import setup_configs
except:
    from .set_configs import setup_configs


def show_env_vars():
    for key in ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "FSSPEC_CONFIG_DIR"): 
        print(f"{key} -> {os.environ[key]}")


def wrap_xr_open(dataset, access_dict=None):
    if access_dict: 
        # construct the input options for fsspec
        fssopts = {
                "key": access_dict["token"],
                "secret": access_dict["secret"],
                "client_kwargs": {"endpoint_url": access_dict["endpoint"]}
        }

    print(fssopts)
    fsspec_config_dir = setup_configs(access_dict["token"], access_dict["secret"], access_dict["endpoint"])

    print("HAVE TO SET CONFIG BEFORE IMPORTING xarray??????")

    # import xarray as xr
    # import fsspec


    ref = "http://cmip6-zarr-o.s3.jc.rl.ac.uk/A-NC-CMIP6.CMIP.MOHC.HadGEM3-GC31-MM/1pctCO2.r1i1p1f3.Amon.hus.gn.v20200115.185001-186912.nc.json"
    ref = "s3://s3-acclim/kerchunk-jsons/CMIP6.CMIP.MOHC.HadGEM3-GC31-MM.1pctCO2.r1i1p1f3.Amon.hus.gn.v20200115-V2.json"
    ref = "s3://s3-acclim/kerchunk-jsons/CMIP6.CMIP.MOHC.HadGEM3-GC31-MM.1pctCO2.r1i1p1f3.Amon.hus.gn.v20200115-V2-fixed-fill_value.json"

    # import s3fs
    # s3fs  = s3fs.S3FileSystem(**fssopts)
    # ref = s3fs.open(dataset)
    ref = s3fs.S3FileSystem(**fssopts).open(dataset)

    show_env_vars()

 #   fsspec = importlib.reload(fsspec)

    #fsspec.config.conf.setdefault('s3', {}).setdefault('client_kwargs', {})['endpoint'] = access_dict["endpoint"]
    mapper = fsspec.get_mapper('reference://', fo=ref, target_protocol="http", **fssopts)
    show_env_vars()

    ds = xr.open_zarr(mapper) #, **fssopts) #, backend_kwargs={'consolidated': False})

    for key in ("AWS_ACCESS_KEY_ID", "FSSPEC_CONFIG_DIR"): print(f"{key} -> {os.environ[key]}")

    return ds

