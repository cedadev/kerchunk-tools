import os
import xarray as xr
import fsspec
import s3fs


def wrap_xr_open(file_uri, scheme="posix", s3_config=None):
    if scheme == "s3" or s3_config:
        return _open_as_s3(file_uri, s3_config)
    else:
        return _open_as_posix(file_uri)


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

    ref = s3fs.S3FileSystem(**fssopts).open(file_uri)
    mapper = fsspec.get_mapper("reference://", fo=ref, target_protocol="http", 
                               remote_options=fssopts)
    return xr.open_zarr(mapper)

