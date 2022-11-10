import os
from urllib.parse import urlparse
import pytest

from minio import Minio

import kerchunk_tools as kct

from tests.common import BASE_DIR, TEST_DATA_POSIX

pytestmark = pytest.mark.skipif(
    not os.path.isdir(BASE_DIR), 
    reason=f"Data dir {BASE_DIR} not mounted.")

access_keys = "token", "secret", "endpoint"
access_dict = {}
ad = access_dict
bucket_id = "kct-test-s3-quobyte-single"
nc_urls = []
mc = None
index_url = None


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


#def test_check_test_access_env_vars_set(setup_access):
#    pass


def test_s3_quobyte_upload_data_files():
    # Create client with access and secret key.
#    endpoint_no_protocol = access_dict["endpoint"].split(":")[-1][2:]
#    m = Minio(endpoint_no_protocol, access_dict["token"], access_dict["secret"], secure=False)

    # Create bucket
#    mc.make_bucket(bucket_id)
         
    # Check bucket made
    assert bucket_id in [bucket.name for bucket in mc.list_buckets()], f"Failed to create bucket: {bucket}"

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


def Otest_s3_quobyte_single_index():
    data_file = nc_urls[0]
    indexer = kct.Indexer(ad["token"], ad["secret"], ad["endpoint"])

    index_url = indexer.create(data_file, bucket_id)
    index_path = urlparse(index_url).path.split("/", 1)[1]

#    assert index_fname in [obj.object_name for objminio.get(index_file), f"Failed to index file at: {index_file}"
    assert file_in_bucket(index_path, bucket_id, recursive=True), f"Failed to index file at: {index_url}"


def partial_test_s3_quobyte_single_read_data(use_access_control=False):
    _access_dict = access_dict if use_access_control else None

    dataset = index_url = "s3://kct-test-s3-quobyte-single/kerchunk-jsons/hus_Amon_HadGEM3-GC31-MM_1pctCO2_r1i1p1f3_gn_185001-186912.json"
    ds = kct.wrap_xr_open(dataset, access_dict=_access_dict)
    assert hasattr(ds, "dims")


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

