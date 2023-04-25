from kerchunk.hdf import SingleHdf5ToZarr
from kerchunk.combine import MultiZarrToZarr
from kerchunk.utils import consolidate
from fsspec.implementations.reference import ReferenceFileSystem

import base64
import json
import jinja2

gen_template = {
    "gen":[],
    "version": 1
}

def checkdims(chunks):
    dims = [0 for d in chunks[0].split('/')[1].split('.')]
    ndims = 0
    
    for chunk in chunks:
        for x, d in enumerate(chunk.split('/')[1].split('.')):
            if int(d) > dims[x]:
                dims[x] = int(d)
    for x in range(len(dims)):
        dims[x] = dims[x] + 1
        if dims[x] > 1:
            ndims += 1
    return dims, ndims

def get_datelike(fname):
    """
    Locate datelike integer strings from the sample filename
    Identify the start- and end-positions of the datelike structures.
    Replace datelike structures with template language.
    """
    dcount = 0
    dsets = []
    dhold = []
    in_date = False
    for index in range(0,len(fname)-1):
        char = fname[index]
        if char + fname[index+1] in ['19','20'] and not in_date: # Simplified for these centuries
            in_date = True
            dhold.append(index)
            dcount = 0
        if char in '0123456789':
            dcount += 1
        if in_date:
            if char not in '0123456789/' and dcount < 4:
                # Abandon date
                dhold = []
                in_date = False
            elif (char not in '0123456789/' or dcount > 8) and (dcount >= 4):
                # Assume valid date
                dhold.append(index)
                dsets.append(dhold)
                dhold = []
                in_date = False
            else:
                pass
    return dsets

def get_url(fnames, index, force_dmode=None):
    """
    Need to get the first and last parts of the filenames that are consistent, then write with template language.
    Use cstring - custom string Template format for all dates, these can be decoded when reading in the kerchunk files.
    """
    if len(fnames) < 2:
        return fnames[0]

    # Find the date-format objects
    datesets = get_datelike(fnames[0])
    # Typically two datesets, one pathbased and one complete
    url = fnames[0][:datesets[0][0]]
    dmode = ''
    if not force_dmode:
        try:
            longdate = fnames[0][datesets[1][0]:datesets[1][1]].replace('/','')
        except IndexError:
            longdate = fnames[0][datesets[0][0]:datesets[0][1]].replace('/','')

        # Determine resolution of dates
        if len(longdate) == 8:
            dmode = 'd'
        elif len(longdate) == 6:
            dmode = 'm'
        else:
            dmode = 'y'
    else:
        dmode = force_dmode

    for x, d in enumerate(datesets):
        date = fnames[0][d[0]:d[1]]
        cstring = '{}'
        if '/' in date:
            cstring += 'p'
        else:
            cstring += 'f'
        cstring += dmode
        cstring += date.replace('/','')
        cstring += index
        if x != len(datesets)-1:
            url += cstring + '{}' + fnames[0][d[1]:datesets[x+1][0]] # From end of this to start of next
        else:
            url += cstring + '{}' + fnames[0][d[1]:]
    return url

def custom_url(templateurl, pr):

    # Expect custom url format
    # {}fd19990101i_pp{}
    cstrings = templateurl.split('{}')
    url = ''
    for i, cs in enumerate(cstrings):
        if i%2==1:
            url += decode_cstring(cs, pr)
        else:
            url += cs
    return url

def decode_cstring(cstring, pr):
    """
    Custom url formatting:
    f - 19990101: full formatting
    p - 1999/01/01: path-based formatting

    example: f19990101i_pp
    """
    fforms = {
        4:"y",
        6:"m",
        8:"d"
    }

    def get_date(initial, count, fmode='d', dmode='d'):
        """
        Calculate new date based on the initial date and the iterator count.
        Uses dmode for different date iteration schemes.
        Uses fmode for date formats.
        dmode and fmode can be equal but would not be in the case of 1999/01/... iterating by day.
        """
        formats = {
            "d":"%Y%m%d",
            "m":"%Y%m",
            "y":"%Y"
        }

        dformat = formats[fmode]
        import datetime.datetime
        dayi = datetime.strptime(initial,dformat)
        if dmode == 'd':
            dayf = dayi + timedelta(days=count)
        elif dmode == 'm':
            dayf = dayi + timedelta(months=count)
        else:
            dayf = dayi + timedelta(years=count)
        return dayf.strftime(dformat)

    mode = cstring[0]
    dmode = cstring[1]
    init = ''
    for c in cstring[2:]:
        if c in '0123456789':
            init += c
    try:
        fmode = fforms[len(init)]
    except:
        print('fmode error: unknown date format')
        raise IndexError
    
    var = cstring[1+len(init):]
    year, month, day = get_date(init, pr[var], fmode=fmode, dmode=dmode)
    if mode == 'f':
        # Full path mode
        return year + month + day
    elif mode == 'p':
        return year + '/' + month + '/' + day
    else:
        print('mode error: unknown init mode')
        raise IndexError

class SHdf5ToZarrCustom(SingleHdf5ToZarr):
    def __init__(
        self, h5f,
        b64vars=None,
        **kwargs):
        super().__init__(
            h5f,
            **kwargs)
        self.b64vars = b64vars

    def _do_inline(self, threshold):
        """Replace chunks for specific variables with the base64 encoding of that variable.

        Original method only encodes if the array size is less than some threshold, while this
        method will always encode specific variables.
        """
        for k, v in self.store.copy().items():
            is_encode = (v[2] < threshold) or (self.b64vars and (k.split('/')[0] in self.b64vars))
            if isinstance(v, list) and is_encode:
                self.input_file.seek(v[1])
                data = self.input_file.read(v[2])
                try:
                    # easiest way to test if data is ascii
                    data.decode("ascii")
                except UnicodeDecodeError:
                    data = b"base64:" + base64.b64encode(data)
                self.store[k] = data
        
class MZarrToZarrCustom(MultiZarrToZarr):
    def __init__(
        self,
        path,
        **kwargs):
        self.reference = None
        super().__init__(
            path,
            **kwargs)

    def translate(self, filename=None, storage_options=None, use_generators=None, remove_dims=None, reference=None):
        """Custom Translator to add generators to the output json file.

        Performs normal translator functions
        """
        if 1 not in self.done:
            self.first_pass()
        if 2 not in self.done:
            self.store_coords()
        if 3 not in self.done:
            self.second_pass()
        if 4 not in self.done:
            if self.postprocess is not None:
                self.out = self.postprocess(self.out)
            self.done.add(4)
        out = consolidate(self.out)

        # Set up custom generator code
        self.reference = reference
        if use_generators:
            out = self.install_generators(out)
        if remove_dims:
            out = self.remove_dimensions(out)
        
        if filename is None:
            return out
        else:
            with fsspec.open(filename, mode="wt", **(storage_options or {})) as f:
                ujson.dump(out, f)

    def access_reference(self):
        """
        Collect variables from netcdf reference file
        Determine chunking dimensions (if chunking() == 2, assume lat/lon)
        """
        from netCDF4 import Dataset

        reference = Dataset(self.reference)
        maxdims = 0
        checkvars = []
        for var in reference.variables.keys():
            dims = reference.variables[var].chunking()
            if dims != 'contiguous':
                ndims = 0
                for dim in dims:
                    if int(dim) > 1:
                        ndims += 1
                if ndims > maxdims:
                    maxdims = ndims
                    checkvars = []
                if ndims == maxdims:
                    checkvars.append(var)

        # Find the number of chunks in lat and lon dimensions
        chunkdimsizes = []
        for dim in reference.variables[checkvars[0]].chunking():
            if dim > 1:
                chunkdimsizes.append(dim)

        ndims = [
            int(reference.dimensions['lat'].size/chunkdimsizes[0]),
            int(reference.dimensions['lon'].size/chunkdimsizes[1]),
        ]

        return checkvars, ndims

    def prepare_chunkarr(self, dims):
        chunkarr = []
        for x in range(dims[0]):
            row = []
            for y in range(dims[1]):
                row.append(0)
            chunkarr.append(row)
        return chunkarr

    def install_generators(self, out):
        import numpy as np
        from scipy import stats
        """
        Generator install stages
        1. Collect all variables that are to be chunked (see find chunk vars):
        - determine chunking dimensions and numbers of chunks for stage 2
        2. Collect missing chunks:
        - select one variable and the first time slice to check all chunks (if there is spatial chunking at all)
        - select one present and one missing chunk and check across all variables and times to check for inconsistencies.
        - As current, collect metadata, chunk sizes etc.
        4. Assemble metadata:
        - Assemble key, url, offset, length and dimensions
        """
        
        checkvars, dims = self.access_reference()
        chunkarr = self.prepare_chunkarr(dims)

        chunkdict = {}
        var_clengths = {}
        nonuniforms = {}

        # Simple quick check for median chunk sizes
        for var in checkvars:
            # sample all chunks from first file
            lengths = []
            for i in range(dims[0]):
                for j in range(dims[1]):
                    key = var + '/0.' + str(i) + '.' + str(j)
                    try:
                        length = out['refs'][key][2]
                        lengths.append(length)
                    except KeyError:
                        pass
            var_clengths[var] = int(stats.mode(np.array(lengths))[0])
        
        # Collect chunk references to determine the numbers and dimensions of chunks

        ### Single Key Pass ###
        for key in out['refs'].keys():
            if '/' in key:
                var, vmeta = key.split('/')
            else:
                vmeta = key
            if '.z' not in vmeta:

                # Detection of missing chunks
                coords = vmeta.split('.')
                # If the variable has the right coord structure and the chunk length is correct.
                if len(coords) == 3 and out['refs'][key][2] == var_clengths[var]:
                    chunkarr[int(coords[1])][int(coords[2])] += 1

                    try:
                        chunkdict[var].append(key)
                    except KeyError:
                        chunkdict[var] = [key]
        print('[INFO] Checking for mask')
        # Check for consistent chunk gap masks
        chunkarr = np.array(chunkarr)

        # Construct chunk mask
        maxmask = (chunkarr[:,:] == np.max(chunkarr))
        
        #is_min = (chunkarr[:,:] == np.min(chunkarr))
        #if not np.all(np.logical_or(is_max, is_min)):
            # Abort as the chunks are inconsistent
            #print('InconsistentMaskAbort')                
            #return None

        
        print('[INFO] Concatenating Gap Array')
        # Concatenate to chunk gap array
        skipchunks = []
        for x, row in enumerate(maxmask):
            for y, item in enumerate(row):
                if item:
                    skipchunks.append('{}.{}'.format(x,y))

        ## Stage 2 - Create simple generators for each variable, knowing that the chunks are uniform in size per variable
        generators = []
        for var in chunkdict.keys():
            print(var)
            # At least one variable is a uniform chunked set
            made_generators = True

            chunks = chunkdict[var]

            ## Need to determine nature of chunking so far within the variables - get the shape of global chunks
            dims, ndims = checkdims(chunks)

            # Chunk along memory axis seamlessly
            if True: # Extra conditional

                # Set up variables
                index = 'i_' + var
                dimsize = 1

                # Set up the key chunk indicator
                key = str(var) + '/'
                for x, dim in enumerate(dims):
                    if x != 0:
                        key += '.'
                    if dim < 2:
                        key += '0'
                    else:
                        key += '{{' + index + '}}'
                        dimsize = dimsize*dim

                # Compile chunks, offsets

                fnames = []
                for chunk in chunks:
                    meta = out['refs'][chunk]
                    offset = str(meta[1])
                    fnames.append(meta[0])
                url = get_url(fnames, index)

                length = str(var_clengths[var])
                dimensions = {index: {"stop": dimsize}}
                                
                generators.append({
                    "key":key,
                    "url":url,
                    "offset":str(offset),
                    "length":str(length),
                    "dimensions":dimensions,
                    "skipchunks": skipchunks,
                    "mswitch":True
                })
            print('[INFO] removing old references')
            for chunk in chunkdict[var]:
                del out['refs'][chunk]
        out['gen'] = generators
        
        return out
    
    def remove_dimensions(self,out):
        """
        Remove self-reference-only dimensions for ease of use

        i.e latitude/longitude removed where we have lat/lon that are the key dimensions and
        latitude and longitude are unused save for self-reference
        """
        remove = []
        dims_vars = list(set(key.split('/')[0] \
                             for key in out['refs'].keys() \
                             if '.' not in key.split('/')[0] ))
        for dv in dims_vars:
            use_count = 0
            for ref in dims_vars:
                refcheck = f'{ref}/.zattrs'
                try:
                    zattrs = json.loads(out['refs'][refcheck])
                    if dv in zattrs['_ARRAY_DIMENSIONS']:
                        use_count += 1
                    if ref == dv and len(zattrs['_ARRAY_DIMENSIONS']) > 1:
                        use_count += 2
                except KeyError:
                    pass # Nonexistent dimension
            if use_count < 2: # i.e only a self referencing dimension, it can be removed
                remove.append(dv)
        new_refs = {}
        for var in out['refs'].keys():
            if var.split('/')[0] not in remove:
                new_refs[var] = out['refs'][var]
        out['refs'] = new_refs
        return out

class CRefFileSystem(ReferenceFileSystem):
    def __init__(
        self,
        fo,
        **kwargs):
        super().__init__(
            fo,
            **kwargs)

    def _process_gen(self, gens):

        out = {}
        for gen in gens:
            dimension = {
                k: v
                if isinstance(v, list)
                else range(v.get("start", 0), v["stop"], v.get("step", 1))
                for k, v in gen["dimensions"].items()
            }
            skipchunks = [
                c for c in gen["skipchunks"]
            ]

            mswitch = ("mswitch" in gen)

            products = (
                dict(zip(dimension.keys(), values))
                for values in itertools.product(*dimension.values())
            )
            for pr in products:

                key = jinja2.Template(gen["key"]).render(**pr, **self.templates)
                # Custom Month switch
                if mswitch:
                    url = custom_url(gen["url"], pr)
                else:
                    url = jinja2.Template(gen["url"]).render(**pr, **self.templates)

                # Custom skipchunks switch
                if key not in skipchunks:
                    if ("offset" in gen) and ("length" in gen):
                        offset = int(
                            jinja2.Template(gen["offset"]).render(**pr, **self.templates)
                        )
                        length = int(
                            jinja2.Template(gen["length"]).render(**pr, **self.templates)
                        )
                        out[key] = [url, offset, length]
                    elif ("offset" in gen) ^ ("length" in gen):
                        raise ValueError(
                            "Both 'offset' and 'length' are required for a "
                            "reference generator entry if either is provided."
                        )
                    else:
                        out[key] = [url]
if __name__ == '__main__':
    print(get_url([
        '/neodc/esacci/land_surface_temperature/data/AQUA_MODIS/L3C/0.01/v3.00/daily/2002/07/04/ESACCI-LST-L3C-LST-MODISA-0.01deg_1DAILY_NIGHT-20020704000000-fv3.00.nc',
        '/neodc/esacci/land_surface_temperature/data/AQUA_MODIS/L3C/0.01/v3.00/daily/2002/07/05/ESACCI-LST-L3C-LST-MODISA-0.01deg_1DAILY_NIGHT-20020705000000-fv3.00.nc'],'i_pp'))