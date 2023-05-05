import types
import warnings
import json
import math
import numpy as np
from fsspec.registry import register_implementation, _import_class, get_filesystem_class
from fsspec.core import _un_chain
from fsspec.mapping import FSMap
from fsspec.implementations.reference import ReferenceFileSystem

_registry = {}

# external, immutable
registry = types.MappingProxyType(_registry)

def assemble_key_varwise(count, dims, vcount):  
    products = []
    for index in range(len(dims)):
        p = 1
        for prod in dims[index+1:]:
            p = p * int(prod)
        products.append(p)

    key = []
    count = math.floor(count / vcount)

    for x,p in enumerate(products):
        key.append(str(int(count//p)))
        count -= p*(count//p)

    return '.'.join(key)

class CRefFileSystem(ReferenceFileSystem):
    def __init__(
        self,
        fo,
        bounds=None,
        **kwargs):
        self.bounds = bounds
        super().__init__(
            fo,
            **kwargs)

    def _process_gen(self, gens):

        out = {}
        if type(gens) != list:
            gens = [gens]
        for gen in gens:
            out.update(self._process_single_gen(gen))
        return out

    def _process_single_gen(self, gen):
        refs = {}
        #if 'varwise' in gen:
            #varwise = gen['varwise']
        #else:
            #return None
        varwise = True
        if varwise:
            chunkcount = 0
        gapcounter, uniquecounter, fsetcounter = 0, 0, 0
        offset = int(gen['start'])

        gapid, gaplength = int(gen['gaps']['ids'][gapcounter]), int(gen['gaps']['lengths'][gapcounter])
        unid, unlength = int(float(gen['unique']['ids'][uniquecounter])), int(float(gen['unique']['lengths'][uniquecounter]))
        if not self.bounds:
            self.bounds = [(0 for i in gen['dims']), tuple(gen['dims'])]
        ndims = len(gen['dims'])

        while chunkcount < int(gen['dimensions']['i']['stop']):
            for vindex, var in enumerate(gen['variables']):
                # Assemble Key
                key = assemble_key_varwise(chunkcount, gen['dims'], len(gen['variables']))
                skip = False
                if key in gen['skipchunks']:
                    if gen['skipchunks'][key][vindex]:
                        # Skip this chunk
                        skip = True
                if not skip:
                    # Check gap ids
                    if chunkcount == gapid:
                        # This chunk is preceded by a gap
                        offset += gaplength
                        gapcounter += 1
                        if gapcounter < len(gen['gaps']['ids']):
                            gapid, gaplength = int(gen['gaps']['ids'][gapcounter]), int(gen['gaps']['lengths'][gapcounter])
                    if chunkcount == unid:
                        # This chunk has a unique length
                        length = unlength
                        uniquecounter += 1
                        if uniquecounter < len(gen['unique']['ids']):
                            unid, unlength = int(float(gen['unique']['ids'][uniquecounter])), int(float(gen['unique']['lengths'][uniquecounter]))
                    else:
                        length = gen['lengths'][vindex]

                    # Get correct filename - simple way for now
                    if int(gen['files'][fsetcounter][0]) < chunkcount and fsetcounter < len(gen['files'])-1:
                        fsetcounter += 1
                    filename = gen['files'][fsetcounter][1]
                    keyparts = key.split('.')
                    #if np.all((keyparts[i] > bounds[0][i] and keyparts[i] < bounds[1][i]) for i in range(ndims)): # This line alone doubles readtimes
                    refs[var + '/' + key] = [filename, offset, length]

                    offset += length
                chunkcount += 1
        return refs

def filesystem(protocol, bounds=None, **storage_options):
    """Instantiate filesystems for given protocol and arguments

    ``storage_options`` are specific to the protocol being chosen, and are
    passed directly to the class.
    """
    if protocol == "arrow_hdfs":
        warnings.warn(
            "The 'arrow_hdfs' protocol has been deprecated and will be "
            "removed in the future. Specify it as 'hdfs'.",
            DeprecationWarning,
        )
    cls = CRefFileSystem
    return cls(bounds=bounds, **storage_options)

def url_to_fs(url, bounds=None, **kwargs):
    """
    Turn fully-qualified and potentially chained URL into filesystem instance

    Parameters
    ----------
    url : str
        The fsspec-compatible URL
    **kwargs: dict
        Extra options that make sense to a particular storage connection, e.g.
        host, port, username, password, etc.

    Returns
    -------
    filesystem : FileSystem
        The new filesystem discovered from ``url`` and created with
        ``**kwargs``.
    urlpath : str
        The file-systems-specific URL for ``url``.
    """
    chain = _un_chain(url, kwargs)
    inkwargs = {}
    # Reverse iterate the chain, creating a nested target_* structure
    for i, ch in enumerate(reversed(chain)):
        urls, protocol, kw = ch
        if i == len(chain) - 1:
            inkwargs = dict(**kw, **inkwargs)
            continue
        inkwargs["target_options"] = dict(**kw, **inkwargs)
        inkwargs["target_protocol"] = protocol
        inkwargs["fo"] = urls
    urlpath, protocol, _ = chain[0]
    fs = filesystem(protocol, bounds=bounds, **inkwargs)
    return fs, urlpath

def get_mapper(
    url="",
    check=False,
    create=False,
    missing_exceptions=None,
    alternate_root=None,
    bounds=None,
    **kwargs,
):
    """Create key-value interface for given URL and options

    The URL will be of the form "protocol://location" and point to the root
    of the mapper required. All keys will be file-names below this location,
    and their values the contents of each key.

    Also accepts compound URLs like zip::s3://bucket/file.zip , see ``fsspec.open``.

    Parameters
    ----------
    url: str
        Root URL of mapping
    check: bool
        Whether to attempt to read from the location before instantiation, to
        check that the mapping does exist
    create: bool
        Whether to make the directory corresponding to the root before
        instantiating
    missing_exceptions: None or tuple
        If given, these exception types will be regarded as missing keys and
        return KeyError when trying to read data. By default, you get
        (FileNotFoundError, IsADirectoryError, NotADirectoryError)
    alternate_root: None or str
        In cases of complex URLs, the parser may fail to pick the correct part
        for the mapper root, so this arg can override

    Returns
    -------
    ``FSMap`` instance, the dict-like key-value store.
    """
    # Removing protocol here - could defer to each open() on the backend
    fs, urlpath = url_to_fs(url, bounds=bounds,**kwargs)
    root = alternate_root if alternate_root is not None else urlpath
    return FSMap(root, fs, check, create, missing_exceptions=missing_exceptions)