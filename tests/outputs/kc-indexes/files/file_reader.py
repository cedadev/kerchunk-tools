import sys
import os
import warnings
warnings.filterwarnings("ignore")
import json
import numpy as np
import kerchunk_tools.xarray_wrapper as wrap_xr

creds_file = f"{os.environ['HOME']}/.s3_config.json"
s3_config = json.load(open(creds_file))

index_uri = sys.argv[1]
ds = wrap_xr.wrap_xr_open(index_uri, s3_config=s3_config)

lst_uk = ds.lst_unc_loc_atm.sel(time="2002-07-04", lat=slice(50,70), lon=slice(-10, 5))
mx = float(lst_uk.max())
assert 0.603 < mx < 0.605

print("[TEST PASSED] Tested and asserted data looks okay in Xarray!")
