import os
import json
import kerchunk.hdf
import fsspec
from urllib.parse import urlparse


class Indexer:

    MAX_INDEXED_ARRAY_SIZE_IN_BYTES = 10000

    def __init__(self, access_token, access_secret, endpoint_url, max_bytes=-1):
        self.access_token, self.access_secret = access_token, access_secret
        self.endpoint_url = endpoint_url

        self.fssopts = {"key": self.access_token, "secret": self.access_secret, 
                        "client_kwargs": {"endpoint_url": self.endpoint_url}}
        self.update_max_bytes(max_bytes)

    def update_max_bytes(self, max_bytes):
        self.max_bytes = max_bytes if max_bytes > 0 else self.MAX_INDEXED_ARRAY_SIZE_IN_BYTES
              

    def _get_output_url(self, bucket_id, url):
        json_name = urlparse(url).path.split("/")[-1][:-3] + ".json"
 #       import pdb; pdb.set_trace()
        #object_name = "kerchunk-jsons/" + url_parts.
        return f"s3://{bucket_id}/kerchunk-jsons/{json_name}"

    def OLD_check_read_permissions(self, url):
        gws = "/gws/nopw/j04/acclim"
        s3_gws_url = f"s3://{bucket_name}"

        fpath = os.path.join(gws, url.replace("s3://s3-acclim/", ""))
        if not int(oct(os.stat(fpath).st_mode)[-1]) > 3:
             raise IOError(f"Must fix global read-access on file system for file: {fpath}")

    def create(self, data_file, bucket_id, max_bytes=-1, xarray_args=None):
        max_bytes = max_bytes if max_bytes > 0 else self.max_bytes
#        data_files = [data_files] if isinstance(data_files, str) else data_files

 #       import pdb; pdb.set_trace()
#        for url in data_files:

# TODO: get rid of this next check - very JASMIN-specific
#            self.check_read_permissions(url)
        file_url = data_file
        url_out = self._get_output_url(bucket_id, file_url)
        print(f"Reading: {file_url}")

        with fsspec.open(file_url, "rb", **self.fssopts) as infss:
            # generate kerchunk and write to buffer
            h5chunks = kerchunk.hdf.SingleHdf5ToZarr(infss, file_url, inline_threshold=max_bytes)

            with fsspec.open(url_out, "wb", **self.fssopts) as out_fss:
                out_fss.write(json.dumps(h5chunks.translate()).encode())

        print(f"Written file: {url_out}")
        return url_out


