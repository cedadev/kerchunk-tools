import os
from urllib.parse import urlparse
import numpy as np

from minio import Minio
import kerchunk_tools as kct
import pytest

from tests.common import BASE_DIR, TEST_DATA_POSIX


pytestmark = pytest.mark.skipif(
    not os.path.isdir(BASE_DIR), 
    reason=f"Data dir {BASE_DIR} not mounted.")

access_keys = "token", "secret", "endpoint"
access_dict = {}
ad = access_dict

bucket_id = "kct-test-s3-quobyte"
single_index_json = "single_index.json"
multi_index_json = "multi_index.json"

nc_urls = []
mc = None


print("""!!!NOTE!!! he async interactions between
      botocore and aiobotocore appeared to lose the 'endpoint_url' setting!!!!
      The solution is to ensure that this file exists: ~/.config/fsspec/conf.json
      
```
S3_TOKEN=token S3_SECRET=secret S3_ENDPOINT=endpoint pytest -v tests/test_workflows/test_workflow_s3_quobyte_single.py
```
      """)

def setup_module():
    if len(access_dict) == len(access_keys):
        # All good
        return

    for key in access_keys:
        env_var_key = f"s3_{key}".upper()
        value = os.environ.get(env_var_key)

        if not value:
            raise Exception(f"Please provide environment variable: '{key}' to run tests.")

        access_dict[key] = value

    global mc

    endpoint_no_protocol = access_dict["endpoint"].split(":")[-1][2:]
    mc = Minio(endpoint_no_protocol, access_dict["token"], access_dict["secret"], secure=False)


def file_in_bucket(fname, bucket_id, recursive=False):
    return fname in [obj.object_name for obj in mc.list_objects(bucket_id, recursive=recursive)]


def test_s3_quobyte_upload_data_files():
    # Create bucket
    mc.make_bucket(bucket_id)
         
    # Check bucket made
    assert bucket_id in [bucket.name for bucket in mc.list_buckets()], f"Failed to create bucket: {bucket_id}"

    # Upload three netCDF files
    for fpath in TEST_DATA_POSIX:
        fname = os.path.basename(fpath)
        bucket_path = f"{bucket_id}/{fname}" 

        nc_urls.append(f"s3://{bucket_path}")
        size = os.path.getsize(fpath)

        # Put object in the bucket
        mc.fput_object(bucket_id, fname, fpath)

        # Assert file uploaded to S3
        assert file_in_bucket(fname, bucket_id)


def test_s3_quobyte_single_index():
    data_file = nc_urls[0]
    indexer = kct.Indexer(ad["token"], ad["secret"], ad["endpoint"])

    index_url = indexer.create(data_file, bucket_id, output_path=single_index_json)
    index_path = urlparse(index_url).path.split("/", 1)[1]

    assert file_in_bucket(index_path, bucket_id, recursive=True), f"Failed to index file at: {index_url}"


def partial_test_s3_quobyte_single_read_data(single=True):
    index_json = single_index_json if single else multi_index_json
    index_url = f"s3://{bucket_id}/{index_json}"

    ds = kct.wrap_xr_open(index_url, access_dict=access_dict)
    return ds


def test_s3_quobyte_single_read_data_secured():
    ds = partial_test_s3_quobyte_single_read_data(single=True)

    subset = ds.sel(time=slice("1855-01-01", "1856-01-01"), lat=slice(20, 40), lon=slice(20, 40))
    print("subset shape", subset.hus.shape)
    assert subset.hus.shape == (12, 19, 36, 24)

    mx = float(subset.hus.max())
    print(f"MAX: {mx}")

    assert np.isclose(mx, 0.0190827)


@pytest.mark.xfail(reason="Data is currently restricted by access control.")
def test_s3_quobyte_single_read_data_open():
    partial_test_s3_quobyte_single_read_data()


def test_s3_quobyte_multiple_index():
    data_files = nc_urls
    indexer = kct.Indexer(ad["token"], ad["secret"], ad["endpoint"])

    index_url = indexer.create(data_files, bucket_id, output_path=multi_index_json)
    index_path = urlparse(index_url).path.split("/", 1)[1]

    assert file_in_bucket(index_path, bucket_id, recursive=True), f"Failed to index file at: {index_url}"


def test_s3_quobyte_multiple_read_data():
    ds = partial_test_s3_quobyte_single_read_data(single=False)
    subset = ds.sel(time=slice("1850-01-01", "1909-01-01"), lat=slice(0, 1), lon=slice(20, 21), plev=1000)
    print("subset shape", subset.hus.shape)
    assert subset.hus.shape == (708, 2, 1)

    mx = float(subset.hus.max())
    print(f"MAX: {mx}")

    assert np.isclose(mx, 0.00000406)


def test_posix_multiple_read_data():
    import xarray as xr
    ds = xr.open_mfdataset(TEST_DATA_POSIX, combine="by_coords", use_cftime=True) #, combine="by_coords")
    subset = ds.sel(time=slice("1850-01-01", "1909-01-01"), lat=slice(0, 1), lon=slice(20, 21), plev=1000)

    print("subset shape", subset.hus.shape)
    assert subset.hus.shape == (708, 2, 1)

    mx = float(subset.hus.max())
    print(f"MAX: {mx}")

    assert np.isclose(mx, 0.00000406)


def teardown_module():
    print("We would delete everything again here.")
