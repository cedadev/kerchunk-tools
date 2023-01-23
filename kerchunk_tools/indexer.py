import os
import json
import fsspec
import kerchunk.hdf
from kerchunk.combine import MultiZarrToZarr

from urllib.parse import urlparse

from .utils import prepare_dir


class Indexer:

    MAX_INDEXED_ARRAY_SIZE_IN_BYTES = 10000

    def __init__(self, s3_config=None, max_bytes=-1, cache_dir=None):
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

        self.cache_dir = cache_dir
        self.update_max_bytes(max_bytes)

    def update_max_bytes(self, max_bytes):
        self.max_bytes = max_bytes if max_bytes > 0 else self.MAX_INDEXED_ARRAY_SIZE_IN_BYTES

    def _get_output_uri(self, prefix, output_path):
        return f"{self.uri_prefix}{prefix}/{output_path}"

    def _kc_read_single_posix(self, file_uri):
        sdfsdf
        return kerchunk.hdf.SingleHdf5ToZarr(file_uri, inline_threshold=self.max_bytes).translate()

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
        file_uris = [file_uris] if isinstance(file_uris, str) else list(file_uris)

        # Loop through data files collecting their metadata
        single_indexes = []

        # Set the reader for accessing files (for S3 or POSIX)
        if self.scheme == "s3":
            reader = self._kc_read_single_s3
        else:
            reader = self._kc_read_single_posix

        # Loop through files either using in-memory or file-cache approach
        if not self.cache_dir:
            # Keep all single Kerchunk indexes in memory
            for file_uri in file_uris:
                print(f"[INFO] Processing: {file_uri}")
                single_indexes.append(reader(file_uri))
        else:
            # Use cache class to cache each Kerchunk file locally (optimised approach)
            maker = SingleKerchunkMakerWithCacheDir(prefix, file_uris, reader, self.cache_dir)
            maker.process() 
            single_indexes = maker.load_cached_jsons()

        # Decide JSON content
        if len(file_uris) == 1:
            json_content = single_indexes[0]
        else:
            json_content = self._build_multizarr(single_indexes)

        json_to_write = json.dumps(json_content).encode()

        # Define output file uri
        output_uri = self._get_output_uri(prefix, output_path)

        if self.scheme == "s3":
            with fsspec.open(output_uri, "wb", **self.fssopts) as kc_file:
                kc_file.write(json_to_write)
        else:
            prepare_dir(os.path.dirname(output_uri))
            with open(output_uri, "wb") as kc_file:
                kc_file.write(json_to_write)

        print(f"[INFO] Written file: {output_uri}")
        return output_uri


class SingleKerchunkMakerWithCacheDir:

    def __init__(self, prefix, file_uris, reader, cache_dir):
        self._prefix = prefix
        self._file_uris = file_uris
        self._reader = reader
        self._dir = cache_dir
    
    def process(self):
        self._cleaned = False
        self.output_files = []

        for file_uri in self._file_uris:
            print(f"[INFO] Processing: {file_uri}")
            cache_file = self._process_file(file_uri)
            self.output_files.append(cache_file)

    def load_cached_jsons(self):
        jsons = [json.load(open(cache_file)) for cache_file in self.output_files]
        return jsons
        
    def _cleanup_after(self, file_uri):
        if self._cleaned: return
        print(f"[WARN] Cleaning up cache after: {file_uri}")
        do_delete = False

        # Loop through all file URIs until there is a match
        for furi in self._file_uris:
            if file_uri == furi:
                do_delete = True
            
            if do_delete:
                for fpath in self._get_file_pair(file_uri):
                    if os.path.isfile(fpath): 
                        os.remove(fpath)

        self._cleaned = True
    
    def _process_file(self, file_uri):
        cache_file, restart_file = self._get_file_pair(file_uri)

        # Clean up cache and restart files when stopped incorrectly on last run 
        if not os.path.isfile(cache_file) or os.path.isfile(restart_file):
            self._cleanup_after(file_uri) 
        elif os.path.isfile(cache_file):
            print(f"[INFO] Using cached file: {cache_file}")
            return cache_file
        
        open(restart_file, "w").write("")

        # Process the file here
        json_content = self._reader(file_uri)
        json.dump(json_content, open(cache_file, "w"))

        print(f"[INFO] Cache file written: {cache_file}")
        os.remove(restart_file)
        return cache_file
    
    def _get_file_pair(self, file_uri):
        return self._get_cache_file(file_uri), self._get_restart_file(file_uri)

    def _create_dir_for(self, item):
        dr = os.path.dirname(item)
        if not os.path.isdir(dr):
            os.makedirs(dr)
        
    def _get_cache_file(self, file_uri):
        fname = file_uri.split("/")[-1]
        cache_file = f"{self._dir}/{self._prefix}/{fname}.json"
        self._create_dir_for(cache_file)
        return cache_file
        
    def _get_restart_file(self, file_uri):
        r_file = self._get_cache_file(file_uri) + ".RESTART"
        self._create_dir_for(r_file)
        return r_file

