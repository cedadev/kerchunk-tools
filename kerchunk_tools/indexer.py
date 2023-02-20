import os
import json
import fsspec
import kerchunk.hdf
from kerchunk.combine import MultiZarrToZarr
import re
import base64
import struct
import cftime

from urllib.parse import urlparse

from .utils import prepare_dir           #had a . before the word before import

_cached_ref_time = {}

def _pid():
    return os.getpid()

def _set_ref_time(t):
    _cached_ref_time[_pid()] = t

def _get_ref_time():
    return _cached_ref_time[_pid()]

def _clear_ref_time():
    del _cached_ref_time[_pid()]


    
def normalise_datetimes(var):
    import datetime
    ref_time = _get_ref_time()
    time_origin = ref_time["time_origin"]
    
    d3=var["time/.zattrs"]
            
    res = d3.split('\n    "units": ', 1)
    new_string = res[1]
    result = re.search('"days since (.*)"', new_string)
    date=(result.group(1))
    
    time_origin_short = (date.split())[0]
    date_short = (date.split())[0]
    
    print(d3)
    
    time_origin_date = datetime.datetime.strptime(time_origin,"%Y-%m-%d %H:%M:%S")
    date_date = datetime.datetime.strptime(date,"%Y-%m-%d %H:%M:%S")
    
    delta = date_date - time_origin_date
    
    days_difference = delta.days

    print(days_difference)


    convert_times(d3, ref_time)

    
    lis=[]
    sub="time/"
    v_time=[s for s in var if sub in s]
    try:
        v_time.remove("time/.zarray")
    except:
        pass
    try:
        v_time.remove("time/.zattrs")
    except:
        pass
    
    for i in v_time:
        val=(var[i])
        this=(decode_base_64(val, sub))
        lis.append(this)
        
        
    lis2=[]
    sub="time_bounds/"
    v_bound=[s for s in var if "time_bounds/" in s]
    try:
        v_bound.remove("time_bounds/.zarray")
    except:
        pass
    try:
        v_bound.remove("time_bounds/.zattrs")
    except:
        pass
        
    for i in v_bound:
        val=(var[i])
        this=(decode_base_64(val, sub))
        lis2.append(this)
    
    
    #print(v_time)
    #print(lis)
    
    #print(v_bound)
    #print(lis2)
    
    
    def convert_times(times, frm_units, frm_calendar, to_units, to_calendar):
        dates = cftime.num2date(times, frm_units, frm_calendar)#gets datetime from int in calender time
        return cftime.date2num(dates, to_units, to_calendar)#convrets back to into in other calender
    
    
    #def date2num(info, date_from):
    #days_since = date_from["time_origin"]
    #units = ("days since", date_since)
    #return cftime.date2num(date, units, calendar)
    
def encode_base_64(val, sub):
    if sub == "time_bound":
        val1 = val[0]
        val2 = val[1]
        v = struct.pack("dd", val1, val2)
        end_val = (b"base64:" + base64.b64encode(v)).decode()
        return(end_val)
    
    if sub == "time":
        v = struct.pack("d", val)
        end_val = (b"base64:" + base64.b64encode(v)).decode()
        return(end_val)

def decode_base_64(val, sub):
    v = val[7:]
    byte_list = base64.b64decode(v)
    if sub == "time/":
        val_list = struct.unpack("d", byte_list)          #https://docs.python.org/3/library/struct.html#format-characters
        val = val_list[0]
        if str(val).endswith('.0'):
            temp_val = str(val)[:-2]
            new_val = int(temp_val)
        else:
            new_val = val
            
        return(new_val)
            
    if sub == "time_bounds/":
        val_list = struct.unpack("dd", byte_list)          #https://docs.python.org/3/library/struct.html#format-characters
        end_list = []
        for val in val_list:
            if str(val).endswith('.0'):
                temp_val = str(val)[:-2]
                new_val = int(temp_val)
                end_list.append(new_val)
            else:
                end_list.append(val)
        return(end_list)
    
    
    
    

class Indexer:

    MAX_INDEXED_ARRAY_SIZE_IN_BYTES = 10000

    def __init__(self, s3_config=None, max_bytes=-1, cache_dir=None, use_time=None):
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
            
        self._prepare_time(ref_time_file=use_time)

        self.cache_dir = cache_dir
        self.update_max_bytes(max_bytes)
        
        
    def _prepare_time(self, ref_time_file=None):
        if not ref_time_file:
            self.ref_time = None
            return

        from netCDF4 import Dataset
        with Dataset(ref_time_file) as ds:

            try:
                time_var = ds.get_variables_by_attributes(standard_name="time")[0]
                self.ref_time = {key: time_var.getncattr(key) for key in time_var.ncattrs()}
                self.ref_time["name"] = time_var.name
            except KeyError as exc:
                raise Exception(f"Could not identify time coordinate in: {fpath}")
            _set_ref_time(self.ref_time)
        
        

    def update_max_bytes(self, max_bytes):
        self.max_bytes = max_bytes if max_bytes > 0 else self.MAX_INDEXED_ARRAY_SIZE_IN_BYTES

    def _get_output_uri(self, prefix, output_path):
        return f"{self.uri_prefix}{prefix}/{output_path}"

    def _kc_read_single_posix(self, file_uri):
        return kerchunk.hdf.SingleHdf5ToZarr(file_uri, inline_threshold=self.max_bytes).translate()

    def _kc_read_single_s3(self, file_uri):
        with fsspec.open(file_uri, "rb", **self.fssopts) as input_fss:
            # generate kerchunk and write to buffer
            return kerchunk.hdf.SingleHdf5ToZarr(input_fss, file_uri, inline_threshold=self.max_bytes).translate()

    def _build_multizarr(self, singles, skip=None):
        kwargs = {}

        if self.scheme == "s3":
            kwargs["remote_protocol"] = "s3"
            kwargs["remote_options"] = self.fssopts
      
        if self.ref_time != None and skip == False:
            mzz = MultiZarrToZarr(singles, concat_dims=["time"], preprocess=normalise_datetimes if self.ref_time else None, **kwargs)
        
        if self.ref_time == None or skip==None:
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

        dates = None
        skip = None
        for item in range(len(file_uris)):
            d=(single_indexes[item])
            d2=d["refs"]
            d3=d2["time/.zattrs"]
            
            res = d3.split('\n    "units": ', 1)
            new_string = res[1]
            result = re.search('"days since (.*)"', new_string)
            date=(result.group(1))
            if dates:
                if dates == date:
                    pass
                else:
                    skip = False
                    
            if dates == None:
                dates = date
                
        if skip == False and self.ref_time == None:
            print("[INFO] THE DATES UNITS ARE DIFFERENT FOR SOME OF THE FILES, YOU MAY WANT TO RUN WITH -t TO NORMALISE THE DATETIMES TO AVOID CONCATINATION ERRORS!!")


        if len(file_uris) == 1:
            json_content = single_indexes[0]
        else:
            json_content = self._build_multizarr(single_indexes, skip)

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
        
        if self.ref_time != None:
            _clear_ref_time()
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

