import os
import pytest

from minio import Minio

import kerchunk_tools as kct

from tests.common import BASE_DIR, TEST_DATA_POSIX

pytestmark = pytest.mark.skipif(
    not os.path.isdir(BASE_DIR), 
    reason=f"Data dir {BASE_DIR} not mounted.")

access_keys = "token", "secret", "endpoint"
access_dict = {}
bucket_id = "kct-test-s3-quobyte-single"


@pytest.fixture
def setup_access():
    if len(access_dict) == len(access_keys):
        # All good
        return

    for key in access_keys:
        env_var_key = f"s3_{key}".upper()
        value = os.environ.get(env_var_key)

        if not value:
            raise Exception(f"Please provide environment variable: '{key}' to run tests.")

        access_dict[key] = value


def test_check_test_access_env_vars_set(setup_access):
    pass


def OFFtest_s3_quobyte_upload_data_files(setup_access):
    # Create client with access and secret key.
    endpoint_no_protocol = access_dict["endpoint"].split(":")[-1][2:]
    m = Minio(endpoint_no_protocol, access_dict["token"], access_dict["secret"], secure=False)

    # Create bucket
    m.make_bucket(bucket_id)
         
    # Check bucket made
    assert bucket_id in [bucket.name for bucket in m.list_buckets()], f"Failed to create bucket: {bucket}"

    # Upload three netCDF files
    for fpath in TEST_DATA_POSIX:
        fname = os.path.basename(fpath)
        bucket_path = f"{bucket_id}/{fname}" 
        size = os.path.getsize(fpath)

        # Put object in the bucket
        m.fput_object(bucket_id, fname, fpath)

        # Assert file uploaded to S3
        assert fname in [obj.object_name for obj in m.list_objects(bucket_id)]

def test_s3_quobyte_single_index(setup_access):
    indexer = kct.Indexer(access_dict)
    indexer.create(data_file, index_file, xarray_args=None)
    
    assert minio.get(index_file), f"Failed to index file at: {index_file}"


def partial_test_s3_quobyte_single_read_data(use_access_control=False):
    ad = access_dict if use_access_control else None
        
    ds = kct.wrap_xr_open(dataset, other, access_dict=ad)
    assert ds == "DOG"


def test_s3_quobyte_single_read_data(setup_access):
    partial_test_s3_quobyte_single_read_data()


@pytest.mark.xfail("Data is currently restricted by access control.")
def test_s3_quobyte_single_read_data_open():
    partial_test_s3_quobyte_single_read_data()


"""
def test_s3_quobyte_multiple_index(setup_access):
    pass


def test_s3_quobyte_multiple_read_data(setup_access):
    pass


def teardown_module():
    print("We would delete everything again here.")

"""

