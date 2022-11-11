import os
import json
import fsspec
import kerchunk.hdf
from kerchunk.combine import MultiZarrToZarr

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

    def _get_output_url(self, bucket_id, output_path):
        return f"s3://{bucket_id}/{output_path}"

    def create(self, file_urls, bucket_id, output_path="index.json", max_bytes=-1, xarray_args=None):
        max_bytes = max_bytes if max_bytes > 0 else self.max_bytes
        file_urls = [file_urls] if isinstance(file_urls, str) else file_urls

        # Loop through data files collecting their metadata
        singles = []

        for url in file_urls:
            print(f"Reading: {url}")

            with fsspec.open(url, "rb", **self.fssopts) as input_fss:
                # generate kerchunk and write to buffer
                h5chunks = kerchunk.hdf.SingleHdf5ToZarr(input_fss, url, inline_threshold=max_bytes)
                singles.append(h5chunks.translate())

        url_out = self._get_output_url(bucket_id, output_path)

        # Write JSON for single file or merge and write for multiple files
        if len(file_urls) == 1:
            json_content = singles[0]
        else:
            mzz = MultiZarrToZarr(
                singles,
                remote_protocol="s3",
                remote_options=self.fssopts,
                concat_dims=["time"]
            )

            json_content = mzz.translate()

        with fsspec.open(url_out, "wb", **self.fssopts) as output_fss:
            output_fss.write(json.dumps(json_content).encode())

        print(f"Written file: {url_out}")
        return url_out