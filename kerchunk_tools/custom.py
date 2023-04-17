from kerchunk.hdf import SingleHdf5ToZarr
from kerchunk.combine import MultiZarrToZarr
from kerchunk.utils import consolidate

import base64
import json

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
    

def get_url(fnames, index):
    """
    Need to get the first and last parts of the filenames that are consistent, then write with template language.
    Assume monthly file format for now. May need to adapt for later.
    """
    if len(fnames) < 2:
        return fnames[0]

    diffs = []
    for x in range(len(fnames[0])):
        if fnames[0][x] != fnames[-1][x] or fnames[0][x] != fnames[-2][x] :
            diffs.append(x)
    part1 = fnames[0][:diffs[0]]
    part2 = fnames[0][diffs[-1]+1:]
    print(part1, part2)

    if len(diffs) == 2: # Months
        
        offset = int(fnames[0][diffs[0]:diffs[-1]])
        step = int(fnames[1][diffs[0]:diffs[-1]]) - offset
    
        url = part1 + "{{ '%02d' % ("
        if offset > 1:
            url += offset + '+' 
        if step > 1:
            url += '(' + index + '*' + step + '))}}' + part2
        else:
            url += '(' + index + '))}}' + part2
            
    elif len(diffs) == 10: # Years
        # Very specific filename structure
        offset = int(fnames[0][diffs[0]:diffs[0]+4])
        year = "{{(" + str(offset) + "+(" + str(index) + "/12))}}"
        month = "{{'%02d' % " + index + "}}"
        url = part1 + year + fnames[0][diffs[3]:diffs[4]] + year + month + part2
    elif len(diffs) == 12: ## Daily years
        # possibly more specific filename structure
        init_year = int(fnames[0][diffs[0]:diffs[0]+4])
        year = "{{(" + str(init_year) + "+(" + str(index) + "/365))}}"
    else:
        print(len(diffs))
        url = ''
    return url
    

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
        super().__init__(
            path,
            **kwargs)

    def translate(self, filename=None, storage_options=None, use_generators=None, remove_dims=None):
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
        ## Additional step of using generators
        if use_generators:
            out = self.install_generators(out)
        if remove_dims:
            out = self.remove_dimensions(out)
        
        if filename is None:
            return out
        else:
            with fsspec.open(filename, mode="wt", **(storage_options or {})) as f:
                ujson.dump(out, f)

    def install_generators(self, out):
        
        ## Stage 1 - Collect and Associate all variable chunks
        ## Also check for chunk uniformity per variable
        ## Doing this in a single pass minimises number of repeats (O(2n) -> O(n))
        
        chunkdict = {}
        var_clengths = {}
        nonuniforms = {}
        
        for key in out['refs'].keys():
            try:
                k = key.split('/')[0]
                x = int(key.split('/')[1].split('.')[0])
                if 'base64' not in out['refs'][key]:
                    try:
                        chunkdict[k].append(key)
                    except KeyError:
                        chunkdict[k] = [key]

                    if k in var_clengths:
                        if out['refs'][key][2] != var_clengths[k]:
                            nonuniforms[k] = False
                    else:
                        var_clengths[k] = out['refs'][key][2]
            except:
                pass
            
        ## Stage 2 - Create simple generators for each variable, knowing that the chunks are uniform in size per variable
        generators = []
        made_generators = False
        for var in chunkdict.keys():
            if var not in nonuniforms:
                # At least one variable is a uniform chunked set
                made_generators = True

                
                chunks = chunkdict[var]
                ## Need to determine nature of chunking so far within the variables - get the shape of global chunks
                dims, ndims = checkdims(chunks)
    
                print('Ndims', ndims)
                if ndims == 1: # Single chunking dimension
                    index = 'i_' + var
                    key = str(var) + '/'
                    maindim = 0
                    for x, dim in enumerate(dims):
                        if x != 0:
                            key += '.'
                        if dim < 2:
                            key += '0'
                        else:
                            key += '{{' + index + '}}'
                            maindim = dim
                    fnames = []
                    for chunk in chunks:
                        meta = out['refs'][chunk]
                        offset = str(meta[1])
                        fnames.append(meta[0])
                    url = get_url(fnames, index)
                    length = str(var_clengths[var])
                    dimensions = {index: {"stop": maindim}}
                                  
                    generators.append({
                        "key":key,
                        "url":url,
                        "offset":str(offset),
                        "length":str(length),
                        "dimensions":dimensions
                    })
                for chunk in chunkdict[var]:
                    del out['refs'][chunk]
        if made_generators:
            out['gen'] = generators
        
        return out
    
    def install_generators_unused(self, out):
        ## My routine for compiling generators in the output

        ## Stage 1 - Collect and Associate all variable chunks
        chunkdict = {}
        for key in out['refs'].keys():
            try:
                x = int(key.split('/')[1].split('.')[0])
                if 'base64' not in out['refs'][key]:
                    try:
                        chunkdict[key.split('/')[0]].append(key)
                    except KeyError:
                        chunkdict[key.split('/')[0]] = [key]
            except:
                pass

        ## Stage 2 - Concatenate sub chunks if any exist while collecting offsets and lengths

        for var in chunkdict.keys():
            offset_size = []
            chunk_sets = {}
            urls = {}


            filedict = {}
            # Collect offsets and lengths for matching chunk files
            for chunk in chunkdict[var]:
                cset = out['refs'][chunk] # get chunk metadata
                fname = cset[0]

            ## Determine number of unique combinations of offsets and sizes
            for chunk in chunkdict[var]:
                cset = out['refs'][chunk] # get chunk metadata
                notadded = True

                # Compare with known combinations and add new ones or record for each chunk the matching combination
                for ord, osset in enumerate(offset_size):
                    if (cset[1], cset[2]) == osset and notadded:
                        chunk_sets[chunk] = ord
                        urls[chunk] = out['refs'][chunk][0]
                        notadded = False
                if notadded:
                    chunk_sets[chunk] = len(offset_size)
                    offset_size.append((cset[1], cset[2]))

                # Now we have a set of unique combinations and a dict of chunks and 'pointers' to the combinations that will become the generators

            # Ideally create a single generator block for all files of the same size and type. 
            # Future addition: Dynamic filename generation using template language
            # For now, each file must be given a separate generator.
            for combo in offset_size:
                
                gen = create_generator(combo)
                        

        ## 1. Identify chunks by timestep.
        ## 2. Determine numbers of unique chunk sizes and offsets
        ##    If all unique, set up a generator for each timestep
        ##    If some are equal, generators can be shared between files.
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
        print(dims_vars)
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
            print(dv, use_count)
            if use_count < 2: # i.e only a self referencing dimension, it can be removed
                remove.append(dv)
        new_refs = {}
        for var in out['refs'].keys():
            if var.split('/')[0] not in remove:
                new_refs[var] = out['refs'][var]
        out['refs'] = new_refs
        return out