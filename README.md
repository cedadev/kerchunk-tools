# kerchunk-tools
Tools to work with kerchunk

## Patching libraries

Had to add this to: aiobotocore/endpoint.py at Line 304

```
        import os
        endpoint_config_key = "S3_ENDPOINT"
        endpoint_url = os.environ.get(endpoint_config_key, "") or endpoint_url
        print(f"Setting endpoint in: aiobotocore/endpoint.py, as: {endpoint_url}")
```

## Testing

```
module load jaspy # or other way to install Python3

python -m venv venv
pip install -e . --no-deps
pip install -r requirements.txt
pip install -r requirements_dev.txt
```

Run tests:

NOTE: I COULD NOT RUN UNIT TESTS WITH `pytest` BECAUSE THE ENV VARS WERE LOST IN THE ASYNC CALLS
      WITHIN THE STACK (I THINK).


```
OVERWRITE_FSSPEC_CONFIG=1 S3_TOKEN=s3_token S3_SECRET=s3_secret S3_ENDPOINT=s3_endpoint pytest tests/test_workflows/test_workflow_s3_quobyte_single.py -v
```

Actually, it is possible to overcome this by NOT using env vars, but ensuring this file exists:


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


AND MAYBE WE CAN SIMPLIFY THE OTHER AUTH STUFF !!!

SO: you need to set this env var to allow the app to overwrite the fsspec config file: OVERWRITE_FSSPEC_CONFIG=1
