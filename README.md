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

### Installing with Pip

Assuming you have Python 3 installed:

```
python -m venv venv
source venv/bin/activate
python -m pip install -r requirements.txt
```

### Installing with miniconda

From scratch, you can conda install with:

```
wget https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh
bash Miniconda3-latest-Linux-x86_64.sh -p ~/miniconda -b

source ~/miniconda/bin/activate
conda create --name kerchunk-tools --file spec-file.txt

conda activate kerchunk-tools
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
OVERWRITE_FSSPEC_CONFIG=1 S3_TOKEN=s3_token S3_SECRET=s3_secret S3_ENDPOINT_URL=s3_endpoint pytest tests/test_workflows/test_workflow_s3_quobyte_single.py -v
```

## NOTE ABOUT HACK FOR KEEPING TRACK OF S3 CONFIG

The S3 Config details appear to get lost in the Xarray-Zarr interface to S3.

The effect is:
 - metadata is read
 - but when it attempts to read data: an internal error means it cannot do so

It is possible to overcome this by NOT using env vars, but ensuring this file exists:


```
$ cat ~/.config/fsspec/conf.json
{
    "s3": {
        "client_kwargs": {
            "endpoint_url": "ENDPOINT_URL" 
        }
    }
}

```

The above overcomes the problem - and doesn't lose track of the environment.

SO: you need to set this env var to allow the app to overwrite the fsspec config file: OVERWRITE_FSSPEC_CONFIG=1


## Performance testing

Our initial tests, having only run once, came out as follows:

Table of test timings (in seconds). Where multiple values appear, the test was run multiple times.

|---------------------|---------------------------|----------------------------|
| Test type           | Read/process small subset | Read/process larger subset |
|---------------------|---------------------------|----------------------------|
| POSIX Kerchunk      |                       0.7 |                      37.9  |
| S3-Quobyte Kerchunk |                       1.1 |                       8.5  |
| S3-DataCore Zarr    |                      |                        |
| POSIX Xarray        |                  0.6, 0.9 |                 86.0, 91.4 |
|---------------------|---------------------------|----------------------------|

We need to run these repeatedly to validate them.

