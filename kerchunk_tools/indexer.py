import os
import json
import fsspec
import kerchunk.hdf
from kerchunk.combine import MultiZarrToZarr

from urllib.parse import urlparse


class Indexer:

    MAX_INDEXED_ARRAY_SIZE_IN_BYTES = 10000

    def __init__(self, s3_config=None, max_bytes=-1):
        if s3_config:
            self.scheme = "s3"
            self.uri_prefix = "s3://"
            self.fssopts = {
                "key": s3_config["token"],
                "secret": s3_config["secret"],
                "client_kwargs": {"endpoint_url": s3_config["endpoint_url"]}
            }

        else:
            self.scheme = "posix"
            self.uri_prefix = ""

        self.update_max_bytes(max_bytes)

    def update_max_bytes(self, max_bytes):
        self.max_bytes = max_bytes if max_bytes > 0 else self.MAX_INDEXED_ARRAY_SIZE_IN_BYTES

    def _get_output_uri(self, prefix, output_path):
        return f"{self.uri_prefix}{prefix}/{output_path}"

    def _kc_read_single_posix(self, file_uri):
        return kerchunk.hdf.SingleHdf5ToZarr(file_uri).translate()

    def _kc_read_single_s3(self, file_uri):
        with fsspec.open(file_uri, "rb", **self.fssopts) as input_fss:
            # generate kerchunk and write to buffer
            return kerchunk.hdf.SingleHdf5ToZarr(input_fss, file_uri, inline_threshold=self.max_bytes).translate()

    def _build_multizarr(self, singles):
        kwargs = {}

        if self.scheme == "s3":
            kwargs["remote_protocol"] = "s3"
            kwargs["remote_options"] = self.fssopts
      
        mzz = MultiZarrToZarr(singles, concat_dims=["time"], **kwargs) 
        return mzz.translate() 

    def create(self, file_uris, prefix, output_path="index.json", max_bytes=-1):
        self.update_max_bytes(max_bytes)
        file_uris = [file_uris] if isinstance(file_uris, str) else file_uris

        # Loop through data files collecting their metadata
        single_indexes = []

        for file_uri in file_uris:
            print(f"Reading: {file_uri}")

            if self.scheme == "s3":
                reader = self._kc_read_single_s3
            else:
                reader = self._kc_read_single_posix

            single_indexes.append(reader(file_uri))

        # Define output file uri
        output_uri = self._get_output_uri(prefix, output_path)

        # Write JSON for single file or merge and write for multiple files
        if len(file_uris) == 1:
            json_content = single_indexes[0]
        else:
            json_content = self._build_multizarr(single_indexes)

        json_to_write = json.dumps(json_content).encode()

        if self.scheme == "s3":
            with fsspec.open(output_uri, "wb", **self.fssopts) as kc_file:
                kc_file.write(json_to_write)
        else:
            with open(output_uri, "wb") as kc_file:
                kc_file.write(json_to_write)

        print(f"Written file: {output_uri}")
        return output_uri

