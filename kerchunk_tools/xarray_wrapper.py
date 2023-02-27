import os
import xarray as xr
import fsspec
import s3fs

_xr_open_args = {"consolidated": False}


def wrap_xr_open(file_uri, scheme="posix", s3_config=None, compression="infer"):
    if compression == "infer":
        compression = "zstd" if file_uri.split(".")[-1].lower() == "zstd" else None

    if scheme == "s3" or s3_config:
        return _open_as_s3(file_uri, s3_config, compression=compression)
    else:
        return _open_as_posix(file_uri, compression=compression)


def _open_as_posix(file_uri, compression=None):
    mapper = fsspec.get_mapper("reference://", fo=file_uri, target_options={"compression": compression})
    return xr.open_zarr(mapper, **_xr_open_args)


def _open_as_s3(file_uri, s3_config, compression=None):
    # construct the input options for fsspec
    fssopts = {
                "key": s3_config["token"],
                "secret": s3_config["secret"],
                "client_kwargs": {"endpoint_url": s3_config["endpoint_url"]}
    }

    ref = s3fs.S3FileSystem(**fssopts).open(file_uri)
    mapper = fsspec.get_mapper("reference://", fo=ref, target_protocol="http", 
                               remote_options=fssopts, target_options={"compression": compression})
    return xr.open_zarr(mapper, **_xr_open_args)


