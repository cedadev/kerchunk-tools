import os
import importlib

import xarray as xr
import fsspec
import s3fs


DEBUG = True

try:
    from set_configs import setup_configs
except:
    from .set_configs import setup_configs


def _show_env_vars():
    if not DEBUG: return
    for key in ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "FSSPEC_CONFIG_DIR"): 
        print(f"{key} -> {os.environ.get(key, 'UNDEFINED')}")


def wrap_xr_open(file_uri, s3_config=None):
    if not s3_config:
        return _open_as_posix(file_uri)
    else:
        return _open_as_s3(file_uri, s3_config)


def _open_as_posix(file_uri):
    mapper = fsspec.get_mapper("reference://", fo=file_uri)
    return xr.open_zarr(mapper) 


def _open_as_s3(file_uri, s3_config):
    # construct the input options for fsspec
    fssopts = {
                "key": s3_config["token"],
                "secret": s3_config["secret"],
                "client_kwargs": {"endpoint_url": s3_config["endpoint_url"]}
    }

    print(fssopts)
    fsspec_config_dir = setup_configs(fssopts["key"], fssopts["secret"], s3_config["endpoint_url"])

    print("HAVE TO SET CONFIG BEFORE IMPORTING xarray??????")
    ref = s3fs.S3FileSystem(**fssopts).open(file_uri)

    _show_env_vars()
    mapper = fsspec.get_mapper('reference://', fo=ref, target_protocol="http", **fssopts)
    _show_env_vars()

    ds = xr.open_zarr(mapper)
    _show_env_vars()
    return ds

