import os
import pytest

import kerchunk_tools as kct

from ..common import BASE_DIR

pytestmark = pytest.mark.skipif(
    not os.path.isdir(BASE_DIR), 
    f"Data dir {BASE_DIR} not mounted.")

access_keys = "token", "secret", "endpoint"
access_dict = {}


@pytest.fixture
def setup_access():
    if len(access_dict) == len(access_keys):
        # All good
        return

    for key in access_keys:
        value = os.environ.get(key)
        if not value:
            raise Exception(f"Please provide environment variable: {key} to run tests.")

        access_dict[key] = value


def test_s3_quobyte_single_index(setup_access):
    indexer = kct.Indexer(access_dict)
    indexer.create(data_file, index_file, xarray_args=None)
    
    assert index file exists at url - using minio, f"Failed to index file at: {index_file}"


def test_s3_quobyte_single_read_data(setup_access):
    ds = kct.wrap_xr_open() 
    ds.

@pytest.mark.xfail("Data is currently restricted by access control.")
def test_s3_quobyte_single_read_data_open():
    test without access control 


def test_s3_quobyte_multiple_index(setup_access):
    indexer = kct.Indexer(access_dict)
    indexer.create(data_file, index_file, xarray_args=None)

    assert index file exists at url - using minio, f"Failed to index file at: {index_file}"


def test_s3_quobyte_multiple_read_data(setup_access):
    ds = kct.wrap_xr_open()
    ds.

