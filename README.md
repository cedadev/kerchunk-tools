# kerchunk-tools

## Overview

This is a set of tools for working with the "kerchunk" library:

 https://fsspec.github.io/kerchunk/

Kerchunk provides cloud-friendly indexing of data files without needing to move
the data itself.

The tools included here allow:
 - indexing of existing NetCDF files to kerchunk files
 - aggregation of existing NetCDF files to a single kerchunk file
 - tools to write to either POSIX file systems or S3-compatible object-store
 - a wrapper around `xarray` to ensure that the data can be read by Python
 - integration with access control to limit read/write operations as desired

## Installation

### Method 1: Install with miniconda

From scratch, you can conda install with:

```bash
wget https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh
bash Miniconda3-latest-Linux-x86_64.sh -p ~/miniconda -b

source ~/miniconda/bin/activate
conda create --name kerchunk-tools --file spec-file.txt

conda activate kerchunk-tools
pip install -e . --no-deps
```

### Method 2: Install with Pip

Assuming you have Python 3 installed, you can also install with Pip:

```bash
python -m venv venv
source venv/bin/activate
pip install --upgrade pip
pip install -r requirements.txt
pip install -e . --no-deps 
```

NOTE: this installation method generated a lot of HDF5 library warnings 
      when reading data, which were not seen with the Conda install.

## Basic usage

Here is an example of using `kerchunk_tools` with authentication to the 
S3 service:

```python
import kerchunk_tools as kct

s3_config = {
    "token": "TOKEN",
    "secret": "SECRET",
    "endpoint_url": "ENDPOINT_URL"
}

# Load a Kerchunk file
index_uri = "s3://kc-indexes/ESACCI-OC-L3S-CHLOR_A-MERGED-1M_MONTHLY_4km_GEO_PML_OCx-fv5.0.json"
ds = kct.wrap_xr_open(index_uri, s3_config=s3_config)

# Look at the metadata
print(ds)
chlor_a = ds.chlor_a

print(ds.shape, ds.dims)

# Look at the data
max_slice = ds.chlor_a.sel(time=slice("2020-01-01", "2020-02-22"), lat=slice(40, 34), lon=slice(20, 23)))
print(float(max_slice))

```

## Testing

If you are connecting to a secured endpoint, then you will need three items for your S3 configuration:
 - `S3_TOKEN`
 - `S3_SECRET`
 - `S3_ENDPOINT_URL`

Then you can run a full workflow that:
 - creates a bucket in S3
 - uploads some NetCDF files to S3
 - creates a kerchunk file in S3 (for a single NetCDF file)
 - creates a kerchunk file in S3 (for an aggregation of multiple NetCDF files)
 - read from the kerchunk files and extract/process a subset of data

```
S3_TOKEN=s3_token S3_SECRET=s3_secret S3_ENDPOINT_URL=s3_endpoint pytest tests/test_workflows/test_workflow_s3_quobyte_single.py -v
```

## Performance testing

Our initial tests, having only run once, came out as follows:

Table of test timings (in seconds). Where multiple values appear, the test was run multiple times.


| Test type           | Read/process small subset | Read/process larger subset |
|---------------------|---------------------------|----------------------------|
| POSIX Kerchunk      |                  1.0, 0.7 |                 15.2, 37.9 |
| S3-Quobyte Kerchunk |             1.1, 4.7, 1.3 |            8.5,  9.1,  5.7 |
| S3-DataCore Zarr    |                  3.9, 3.8 |                 99.8, 99.2 |
| POSIX Xarray        |                  0.6, 0.9 |                 86.0, 91.4 |

We need to run these repeatedly to validate them.

### Test types

The test types are:
1. POSIX Kerchunk:
  - This uses a Kerchunk index file on the POSIX file system
  - It references NetCDF files on the POSIX file system
  - There is no use of object-store
  - This test depends on having pre-generated the Kerchunk index file
2. S3-Quobyte Kerchunk:
  - This uses a Kerchunk index file in the JASMIN S3-Quobyte object-store
  - It references NetCDF files in the S3-Quobyte object-store 
    - The files are actually part of the CEDA Archive and are exposed via an S3 interface
  - There is no use of the POSIX file systems
  - This test depends on having pre-generated the Kerchunk index file
3. S3-DataCore Zarr:
  - This reads a Zarr file that we have copied into the JASMIN DataCore (formerly Caringo) object-store
  - The data is the same content as used for the other tests, converted from NetCDF to Zarr
  - There is no use of Kerchunk
  - This test depends on having pre-generated the Zarr file from NetCDF
4. POSIX Xarray:
  - This reads all the NetCDF files directly into Xarray (as a list of files)
  - The files are read directly from the POSIX file system 
  - There is no pre-generation step for this test
  - This is slower because the aggregation of the NetCDF content is done on-the-fly

### Test data

The test data, being used is a list of 279 data files from the CCI archive, under the directory:

```
/neodc/esacci/ocean_colour/data/v5.0-release/geographic/netcdf/chlor_a/monthly/v5.0/
```

The first and last files are:

```
First: .../1997/ESACCI-OC-L3S-CHLOR_A-MERGED-1M_MONTHLY_4km_GEO_PML_OCx-199709-fv5.0.nc 
Last:  .../2020/ESACCI-OC-L3S-CHLOR_A-MERGED-1M_MONTHLY_4km_GEO_PML_OCx-202011-fv5.0.nc 
```

### Test details

In all cases the test is run as follows.

Test 1 - Read/process small subset:

1. Load the data as an `xarray.Dataset` object.
2. Create a small time/lat/lon slice of shape: `(2, 144, 72)` (only 2 time steps == 2 files)
3. Calculate the maximum value and assert it equals the expected value.     s = time.time()

Test 2 - Read/process larger subset:

1. Load the data as an `xarray.Dataset` object.
2. Create a larger time/lat/lon slice of shape: `(279, 12, 24)` (279 time steps == 279 files)
3. Calculate the maximum value and assert it equals the expected value. 

