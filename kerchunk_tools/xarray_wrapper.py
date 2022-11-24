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
        print(f"{key} -> {os.environ.get(key, 'UNDEFINED')}")


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
    ref = s3fs.S3FileSystem(**fssopts).open(dataset)

    show_env_vars()
    #fsspec.config.conf.setdefault('s3', {}).setdefault('client_kwargs', {})['endpoint'] = access_dict["endpoint"]
    mapper = fsspec.get_mapper('reference://', fo=ref, target_protocol="http", **fssopts)
    show_env_vars()

    ds = xr.open_zarr(mapper) #, storage_options=fssopts) 
        #, **fssopts) #, backend_kwargs={'consolidated': False})

    show_env_vars()
    return ds

