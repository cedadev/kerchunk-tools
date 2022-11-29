import os
from urllib.parse import urlparse
import numpy as np

from minio import Minio
import kerchunk_tools as kct
import pytest
import xarray as xr

from tests.common import BASE_DIR, TEST_DATA_POSIX


pytestmark = pytest.mark.skipif(
    not os.path.isdir(BASE_DIR), 
    reason=f"Data dir {BASE_DIR} not mounted.")

prefix = "tests/kct-test-posix-7"
single_index_json = "single_index.json"
multi_index_json = "multi_index.json"

file_uris = TEST_DATA_POSIX


def setup_module():
    if not os.path.isdir(prefix):
        os.mkdir(prefix)


def test_posix_single_index():
    data_file = file_uris[0]
    indexer = kct.Indexer()

    index_uri = indexer.create(data_file, prefix, output_path=single_index_json)
    index_path = index_uri
    assert os.path.isfile(index_path), f"Failed to index file at: {index_uri}"


def partial_test_posix_single_read_data(single=True):
    index_json = single_index_json if single else multi_index_json
    index_uri = f"{prefix}/{index_json}"

    ds = kct.wrap_xr_open(index_uri)
    return ds


def test_posix_single_read_data_secured():
    ds = partial_test_posix_single_read_data(single=True)

    subset = ds.sel(time=slice("1855-01-01", "1856-01-01"), lat=slice(20, 40), lon=slice(20, 40))
    print("subset shape", subset.hus.shape)
    assert subset.hus.shape == (12, 19, 36, 24)

    mx = float(subset.hus.max())
    print(f"MAX: {mx}")

    assert np.isclose(mx, 0.0190827)


def test_posix_multiple_index():
    indexer = kct.Indexer()

    index_uri = indexer.create(file_uris, prefix, output_path=multi_index_json)
    index_path = index_uri

    assert os.path.isfile(index_path), f"Failed to index file at: {index_uri}"


def test_posix_multiple_read_data():
    ds = partial_test_posix_single_read_data(single=False)
    subset = ds.sel(time=slice("1850-01-01", "1909-01-01"), lat=slice(0, 1), lon=slice(20, 21), plev=1000)
    print("subset shape", subset.hus.shape)
    assert subset.hus.shape == (708, 2, 1)

    mx = float(subset.hus.max())
    print(f"MAX: {mx}")

    assert np.isclose(mx, 0.00000406)


def test_posix_read_xarray_as_mfdataset():
    ds = xr.open_mfdataset(TEST_DATA_POSIX, combine="by_coords", use_cftime=True)
    subset = ds.sel(time=slice("1850-01-01", "1909-01-01"), lat=slice(0, 1), lon=slice(20, 21), plev=1000)

    print("subset shape", subset.hus.shape)
    assert subset.hus.shape == (708, 2, 1)

    mx = float(subset.hus.max())
    print(f"MAX: {mx}")

    assert np.isclose(mx, 0.00000406)


def teardown_module():
    print("We would delete everything again here.")
