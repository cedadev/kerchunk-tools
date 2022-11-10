import os
import tempfile
import importlib

fsspec_config_dir = os.path.join(os.environ["HOME"], ".fsspec-tmp-configs")
fsspec_config_fname = "fsspec.json"


def setup_configs(key, secret, url, purge=True):
    if purge: purge_old_configs()
    if not os.path.isdir(fsspec_config_dir):
        os.mkdir(fsspec_config_dir)
    tmpdir = tempfile.mkdtemp(dir=os.path.join(fsspec_config_dir))
#    aws_file = os.path.join(tmpdir, "aws.conf")

#    with open(aws_file, "w") as aws:
#        aws.write(f"""[default]
#aws_access_key_id = {key}
#aws_secret_access_key = {secret}
#""")

    fsspec_file = os.path.join(tmpdir, fsspec_config_fname)
    with open(fsspec_file, "w") as fss:
        fss.write(f"""{{
    "s3": {{
        "client_kwargs": {{
            "endpoint_url": "{url}"
        }}
    }}
}}
""")
 
#    os.environ["AWS_CONFIG_FILE"] = aws_file
    os.environ["FSSPEC_CONFIG_DIR"] = tmpdir
    os.environ["AWS_ACCESS_KEY_ID"] = key
    os.environ["AWS_SECRET_ACCESS_KEY"] = secret

# AWS_CONFIG_FILE=/home/users/astephens/xarray-zarr-deep-dive/tmpconf/aws.conf FSSPEC_CONFIG_DIR=/home/users/astephens/xarray-zarr-deep-dive/tmpconf
    return tmpdir


def purge_old_configs():
    if os.path.isdir(fsspec_config_dir):
        for dr in os.listdir(fsspec_config_dir):

            config_dir = os.path.join(fsspec_config_dir, dr)
            config_file = os.path.join(config_dir, fsspec_config_fname)
            if os.path.isfile(config_file):
                os.remove(config_file)

            os.rmdir(config_dir)
            print(f"[INFO] Purged old fsspec config dir: {config_dir}")
            

def test_setup_configs():
    tmpdir = setup_configs(key="key", secret="secret", url="url")
    print(tmpdir)
    assert os.path.isdir(tmpdir)


#test_setup_configs()

