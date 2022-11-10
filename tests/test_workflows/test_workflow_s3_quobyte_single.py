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
bucket_id = "kct-test-s3-quobyte-1"
nc_urls = []
mc = None
index_url = None


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
#    mc.make_bucket(bucket_id)
         
    # Check bucket made
    assert bucket_id in [bucket.name for bucket in mc.list_buckets()], f"Failed to create bucket: {bucket_id}"

    # Upload three netCDF files
    for fpath in TEST_DATA_POSIX:
        fname = os.path.basename(fpath)
        bucket_path = f"{bucket_id}/{fname}" 

        nc_urls.append(f"s3://{bucket_path}")
        size = os.path.getsize(fpath)

        # Put object in the bucket
#        mc.fput_object(bucket_id, fname, fpath)

        # Assert file uploaded to S3
        assert file_in_bucket(fname, bucket_id)


def test_s3_quobyte_single_index():
    data_file = nc_urls[0]
    indexer = kct.Indexer(ad["token"], ad["secret"], ad["endpoint"])

    global index_url
    index_url = indexer.create(data_file, bucket_id)
    index_path = urlparse(index_url).path.split("/", 1)[1]

    assert file_in_bucket(index_path, bucket_id, recursive=True), f"Failed to index file at: {index_url}"


def partial_test_s3_quobyte_single_read_data(use_access_control=False):
    _access_dict = access_dict if use_access_control else None

    dataset = "s3://kct-test-s3-quobyte-single/kerchunk-jsons/hus_Amon_HadGEM3-GC31-MM_1pctCO2_r1i1p1f3_gn_185001-186912.json"
    index_url = f"s3://{bucket_id}/kerchunk-jsons/hus_Amon_HadGEM3-GC31-MM_1pctCO2_r1i1p1f3_gn_185001-186912.json"
    ds = kct.wrap_xr_open(index_url, access_dict=_access_dict)

    subset = ds.sel(time=slice("1855-01-01", "1856-01-01"), lat=slice(20, 40), lon=slice(20, 40))
    print("subset shape", subset.hus.shape)
    assert subset.hus.shape == (12, 19, 36, 24)

    mx = float(subset.hus.max())
    print(f"MAX: {mx}")

    assert np.isclose(mx, 0.0190827)


def test_s3_quobyte_single_read_data_secured():
    partial_test_s3_quobyte_single_read_data(use_access_control=True)


@pytest.mark.xfail(reason="Data is currently restricted by access control.")
def test_s3_quobyte_single_read_data_open():
    partial_test_s3_quobyte_single_read_data()


"""
def test_s3_quobyte_multiple_index():
    pass


def test_s3_quobyte_multiple_read_data():
    pass


def teardown_module():
    print("We would delete everything again here.")

"""

# def run_all():
#     setup_module()
#     #test_s3_quobyte_upload_data_files()
#     #test_s3_quobyte_single_index()
#     test_s3_quobyte_single_read_data_secured()
#     # test_s3_quobyte_single_read_data_open()
#     # test_s3_quobyte_multiple_index()
#     # test_s3_quobyte_multiple_read_data()
#     # teardown_module()

