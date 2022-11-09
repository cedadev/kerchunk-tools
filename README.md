# kerchunk-tools
Tools to work with kerchunk

## Testing

```
module load jaspy # or other way to install Python3

python -m venv venv
pip install -e . --no-deps
pip install -r requirements.txt
pip install -r requirements_dev.txt
```

Run tests:

```
S3_TOKEN=s3_token S3_SECRET=s3_secret S3_ENDPOINT=s3_endpoint pytest tests/test_workflows/test_workflow_s3_quobyte_single.py -v
```


