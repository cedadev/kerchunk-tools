from kerchunk.hdf import SingleHdf5ToZarr
from kerchunk.combine import MultiZarrToZarr
from kerchunk.utils import consolidate

import base64
import json
import jinja2
import numpy as np
from scipy import stats

class SHdf5ToZarrCustom(SingleHdf5ToZarr):
    """
    Custom class overriding SingleHdf5toZarr class from kerchunk library

    Allows for replacement of specific variables to base64 encoding rather than
    chunkwise storage like other variables
    """
    
    def __init__(
        self, h5f,
        b64vars=None,
        **kwargs):
        super().__init__(
            h5f,
            **kwargs)
        # Single additional init step.
        self.b64vars = b64vars

    def _do_inline(self, threshold):
        """
        Replace chunks for specific variables with the base64 encoding of that variable.

        Original method only encodes if the array size is less than some threshold, while this
        method will always encode specific variables.
        """
        for k, v in self.store.copy().items():
            is_encode = (v[2] < threshold) or (self.b64vars and (k.split('/')[0] in self.b64vars))

            # Encode if variable size lower than threshold or variables are specified in b64vars
            if isinstance(v, list) and is_encode:
                self.input_file.seek(v[1])
                data = self.input_file.read(v[2])
                try:
                    data.decode("ascii")
                except UnicodeDecodeError:
                    data = b"base64:" + base64.b64encode(data)
                self.store[k] = data
        
class MZarrToZarrCustom(MultiZarrToZarr):
    """
    Custom class overriding MultiZarrToZarr class from kerchunk library

    Added generator packing functions to install generators.
    Also added remove dimensions function to remove specified dimension variables or ones that are only self-referenced.
    """
    
    def __init__(
        self,
        path,
        **kwargs):
        # Initial declaration of new variable
        self.reference = None
        super().__init__(
            path,
            **kwargs)

    def translate(self, filename=None, storage_options=None, use_generators=None, remove_dims=None, reference=None):
        """Custom Translator to add generators to the output json file.

        Performs normal translator functions but with two new options:
         - installation of generators
         - removal of self referencing dimensions
        """
        import time
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
        print('[INFO] Starting Custom Section')
        time_dim = json.loads(self.out['time/.zarray'])['chunks'][0]
        variables, dims = self.access_reference(reference, time_dim=time_dim)
        if use_generators:
            t1 = time.perf_counter()
            out = self.install_generators(out, variables, dims)
            print(time.perf_counter() - t1, 's')
        if remove_dims:
            out = self.remove_dimensions(out, variables)
        
        if filename is None:
            return out
        else:
            with fsspec.open(filename, mode="wt", **(storage_options or {})) as f:
                ujson.dump(out, f)

    def access_reference(self, freference, time_dim=1):
        """
        Collect variables from netcdf reference file
        Determine chunking dimensions (if chunking() == 2, assume lat/lon)
        """
        from netCDF4 import Dataset
        print('[INFO] Accessing Reference')
        ignore = ['lat','lon','latitude','longitude','time']
        reference = Dataset(freference)
        maxdims = 0
        checkvars = {}
        for var in reference.variables.keys():
            if var not in ignore:
                dims = reference.variables[var].chunking()
                if dims != 'contiguous':
                    ndims = 0
                    for dim in dims:
                        if int(dim) > 1:
                            ndims += 1
                    key = ndims
                    if maxdims < ndims:
                        maxdims = ndims
                else:
                    key = dims
                if key in checkvars:
                    checkvars[key].append(var)
                else:
                    checkvars[key] = [var]
        if maxdims == 0:
            variables = checkvars['contiguous']
        else:
            variables = False
            while maxdims > 1 and not variables:
                if maxdims in checkvars:
                    variables = checkvars[maxdims]
                    keepdims = maxdims
                maxdims -= 1
        dims = reference.variables[variables[0]].dimensions

        ndims = []
        chunks = reference.variables[variables[0]].chunking()
        for i, dim in enumerate(dims):
            if i == 0:
                ndims.append(time_dim)
            else:
                if chunks != 'contiguous':
                    ndims.append(int( int(reference.dimensions[dim].size) / int(chunks[i]) ))
                else:
                    ndims.append(1)
        return variables, ndims

    def install_generators(self, out, variables, dims):
        """
        Pack chunk arrays into custom generators.

        Single chunk reference pass followed by analysis of lengths and offsets
        Use of numpy arrays rather than python lists to improve performance.
        """
        
        
        def update(countdim, dims, index):
            countdim[index] += 1
            if countdim[index] >= dims[index]:
                countdim[index] = 0
                countdim = update(countdim, dims, index-1)
            return countdim

        refs = out['refs']
        
        countdim = [0 for dim in dims]
        countdim[-1] = -1
        maxdims = [dim-1 for dim in dims]
        chunkindex = 0
        files, skipchunks = [], {}
        filepointer = 0
        ## Stage 1 pass ##
        # - collect skipchunks
        # - collect files
        # - collect offsets and lengths for determining lengths
        print('[INFO] Installing Generator')
        lengths, offsets = [],[]
        while countdim != maxdims:
            countdim = update(countdim, dims, -1)
            # Iterate over dimensions and variables, checking each reference in turn
            for vindex, var in enumerate(variables):
                key = var + '/' + '.'.join(str(cd) for cd in countdim)
                # Compile Skipchunks
                if key not in refs:
                    try:
                        skipchunks['.'.join(str(cd) for cd in countdim)][vindex] = 1
                    except:
                        skipchunks['.'.join(str(cd) for cd in countdim)] = [0 for v in variables]
                        skipchunks['.'.join(str(cd) for cd in countdim)][vindex] = 1
                    try:
                        offsets.append(offsets[-1] + lengths[-1])
                    except:
                        offsets.append(0)
                    lengths.append(0)
                else:
                    lengths.append(refs[key][2])
                    offsets.append(refs[key][1])
                    filename = refs[key][0]
                    if len(files) == 0:
                        files.append([-1, filename])
                    if filename != files[filepointer][1]:
                        files[filepointer][0] = chunkindex-1
                        filepointer += 1
                        files.append([chunkindex, filename])
                    del out['refs'][key]
                chunkindex += 1
        files[-1][0] = chunkindex
        
        lengths = np.array(lengths, dtype=int)
        offsets = np.array(offsets, dtype=int)

        nzlengths = lengths[lengths!=0]

        slengths = [
            int(stats.mode(
                nzlengths[v::len(variables)]
            )[0]) 
            for v in range(len(variables)) ] # Calculate standard chunk sizes

        # Currently have files, variables, varwise, skipchunks, dims, start, lengths, dimensions
        # Still need to construct unique, gaps

        lv = len(variables)
        uniquelengths, uniqueids = np.array([]), np.array([])
        positions = np.arange(0, len(lengths), 1, dtype=int)
        additions = np.roll((offsets + lengths), 1)
        additions[0] = offsets[0] # Reset initial position

        gaplengths = (offsets - additions)[(offsets - additions) != 0]
        gapids     = positions[(offsets - additions) != 0]

        #print(list(lengths[1:1200:lv][lengths[1:1200:lv] != slengths[1]]))
        #print(list(positions[1:1200:lv][lengths[1:1200:lv] != slengths[1]]))

        for v in range(lv):
            q = lengths[v::lv][lengths[v::lv] != slengths[v]]
            p = positions[v::lv][lengths[v::lv] != slengths[v]]
            p = p[q!=0]
            q = q[q!=0]
            uniquelengths = np.concatenate((
                uniquelengths,
                q
            ))
            uniqueids = np.concatenate((
                uniqueids,
                p
            ))
            gapmask    = np.abs(gaplengths) != slengths[v]
            gaplengths = gaplengths[gapmask]
            gapids     = gapids[gapmask]

        sortind = np.argsort(uniqueids)
        uniqueids = uniqueids[sortind]
        uniquelengths = uniquelengths[sortind]

        out['gen'] = {
            "files": files,
            "variables" : list(variables),
            "varwise" : True,
            "skipchunks" : skipchunks,
            "dims" : dims,
            "unique": {
                "ids": [str(id) for id in uniqueids],
                "lengths": [str(length) for length in uniquelengths],
            },
            "gaps": {
                "ids": [str(id) for id in gapids],
                "lengths": [str(length) for length in gaplengths],
            },
            "start": str(offsets[0]),
            "lengths": list(slengths),
            "dimensions" : {
                "i":{
                    "stop": str(chunkindex)
                }
            },
            "gfactor": str( 1 - (len(skipchunks) + len(uniqueids) + len(gapids))/chunkindex)[:4],
        }
        print('[INFO] Installed Generator')

        return out

    def remove_dimensions(self, out, variables):
        """
        Remove self-reference-only dimensions for ease of use

        i.e latitude/longitude removed where we have lat/lon that are the key dimensions and
        latitude and longitude are unused save for self-reference
        """
        print('[INFO] Removing SRO Dimensions')

        remove = []
        dims_vars = list(set(key.split('/')[0] \
                             for key in out['refs'].keys() \
                             if '.' not in key.split('/')[0] and key.split('/')[0] not in variables ))
        for dv in dims_vars:
            use_count = 0
            for ref in variables:
                refcheck = f'{ref}/.zattrs'
                try:
                    zattrs = json.loads(out['refs'][refcheck])
                    if dv in zattrs['_ARRAY_DIMENSIONS']:
                        use_count += 1
                except KeyError:
                    pass # Nonexistent dimension
            if use_count < 1: # i.e only a self referencing dimension, it can be removed
                remove.append(dv)
        new_refs = {}
        for var in out['refs'].keys():
            if var.split('/')[0] not in remove:
                new_refs[var] = out['refs'][var]
        out['refs'] = new_refs

        print('[INFO] Removed Dimensions')
        return out

if __name__ == '__main__':
    print(__file__)