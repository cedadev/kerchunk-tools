import os

from .set_configs import setup_configs


def show_env_vars():
    for key in ("AWS_ACCESS_KEY_ID", "FSSPEC_CONFIG_DIR"): print(f"{key} -> {os.environ[key]}")


def wrap_xr_open(dataset, access_dict=None):
    if access_dict: 
        # construct the input options for fsspec
        fssopts = {
                "key": access_dict["token"],
                "secret": access_dict["secret"],
                "client_kwargs": {"endpoint_url": access_dict["endpoint"]}
        }


    setup_configs(access_dict["token"], access_dict["secret"], access_dict["endpoint"])

    print("HAVE TO SET CONFIG BEFORE IMPORTING xarray??????")

    import xarray as xr
    import fsspec
    import s3fs
    
    s3fs  = s3fs.S3FileSystem(**fssopts)
    ref = s3fs.open(dataset)

    show_env_vars()

    mapper = fsspec.get_mapper('reference://', fo=ref, target_protocol="http", **fssopts)
    show_env_vars()

    ds = xr.open_zarr(mapper) #, **fssopts) #, backend_kwargs={'consolidated': False})
    return ds