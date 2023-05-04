import math
import numpy as np
from scipy import stats
"""
Current Generator Structure:
gen:
 - key: template
 - url: template
 - offset: formula
 - length: formula
 - dimensions: iterators


 Proposed New Structure:
 gen:
  - files      : array of tuples
  - variables  : array of strings
  - varwise    : boolean switch
  - skipchunks : dict of arrays ('0.0.0':[1, 1, 0, 0, 1])
  - dims       : array of dimensions
  - unique:
    - ids     : array of indexes of unique chunks
    - lengths : array of unique lengths of chunks (corresponds to IDs)
  - gaps:
    - ids     : array of indexes with a preceding gap
    - gaps    : array of sizes of preceding gaps
  - commonlength : array of chunk sizes for variables
  - dimensions   : iterator
"""

files = [
    (200,'file1.nc'),
    (400, 'file2.nc')
]

variables = ['water','o2']
skipchunks = {
    '0.0.1': [True, True],
    '0.1.0': [False, True],
    '0.2.0': [True, False]
}

clens = [120, 239]

start = 23136 # byte start

dims = [2, 10, 10]

uniqueids = [15, 31, 48, None]
uniquelengths = [189, 241, 327]

gapids = [41, 89, 112, None]
gaplengths = [337, 337, 337]

iterator = {
    "i": {
        "stop": 400
    }
}

gen = {
    "files": files,
    "variables" : variables,
    "varwise" : True,
    "skipchunks" : skipchunks,
    "dims" : dims,
    "unique": {
        "ids": uniqueids,
        "lengths": uniquelengths,
    },
    "gaps": {
        "ids": gapids,
        "lengths": gaplengths,
    },
    "start": start,
    "lengths": clens,
    "dimensions" : iterator
}

import json
f = open('input.json','w')
f.write(json.dumps(gen))
f.close()



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

def _process_gen(gen):
    refs = {}
    
    f = open('kc-indexes/e_og.json','r')
    reference = json.load(f)
    f.close()

    varwise = True
    if varwise:
        chunkcount = 0
        gapcounter, uniquecounter, fsetcounter = 0, 0, 0
        print(gen.keys())
        offset = int(gen['start'])

        match, fail, extra = 0, 0, 0

        gapid, gaplength = int(gen['gaps']['ids'][gapcounter]), int(gen['gaps']['lengths'][gapcounter])
        unid, unlength = int(float(gen['unique']['ids'][uniquecounter])), int(float(gen['unique']['lengths'][uniquecounter]))

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
                    refs[var + '/' + key] = [filename, offset, length]
                    if var + '/' + key in reference['refs']:
                        if reference['refs'][var + '/' + key] == [filename, offset, length]:
                            match += 1
                        else:
                            fail += 1
                    else:
                        extra += 1

                    offset += length
                chunkcount += 1

    print('Success Rate:')
    print('Matches: ',match)
    print('Mismatches: ',fail)
    print('Extra Keys: ',extra)
    return refs

def pack_generator(out):

    def update(countdim, dims, index):
        countdim[index] += 1
        if countdim[index] >= dims[index]:
            countdim[index] = 0
            countdim = update(countdim, dims, index-1)
        return countdim

    refs = out['refs']
    variables, dims = ['water','o2'], [2,10,10]
    countdim = [0 for dim in dims]
    countdim[-1] = -1
    maxdims = [dim-1 for dim in dims]
    chunkindex = 0
    files = []
    filepointer = 0
    ## Stage 1 pass ##
    # - collect skipchunks
    # - collect files
    # - collect offsets and lengths for determining lengths

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
                offsets.append(offsets[-1] + lengths[-1])
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

    slengths = [
        int(stats.mode(
            lengths[v::len(variables)]
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

    print(offsets-additions)


    for v in range(lv):
        uniquelengths = np.concatenate((
            uniquelengths,
            lengths[v::lv][lengths[v::lv] != slengths[v]]
        ))
        uniqueids = np.concatenate((
            uniqueids,
            positions[v::lv][lengths[v::lv] != slengths[v]]
        ))
        gapmask    = np.abs(gaplengths) != slengths[v]
        gaplengths = gaplengths[gapmask]
        gapids     = gapids[gapmask]

    skipmask = uniquelengths != 0

    uniquelengths = uniquelengths[skipmask]
    uniqueids = uniqueids[skipmask]

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
        }
    }
    return out

if False: # Repack
    g = open('unpacked.json','r')
    out = json.load(g)
    g.close()
    gen = pack_generator({'refs':out})
    f = open('repacked.json','w')
    f.write(json.dumps(gen))
    f.close()
else: # Unpack

    g = open('kc-indexes/e5_gen.json','r')
    out = json.load(g)
    g.close()
    result = _process_gen(out['gen'])
    out['refs'].update(result)
    del out['gen']

    f = open('unpackedfull.json','w')
    f.write(json.dumps(out))
    f.close()

