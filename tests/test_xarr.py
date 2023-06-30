import xarray as xr
import sys
from netCDF4 import Dataset

paths = [
    "/badc/cmip6/data/CMIP6/CMIP/HAMMOZ-Consortium/MPI-ESM-1-2-HAM/historical/r1i1p1f1/Oyr/o2/gn/latest/o2_Oyr_MPI-ESM-1-2-HAM_historical_r1i1p1f1_gn_1850-1869.nc",
    "/home/users/dwest77/Documents/kerchunk/ESACCI/2002/ESACCI-LST-L3C-LST-MODISA-0.01deg_1DAILY_DAY-20020704000000-fv3.00.nc",
    "/badc/cmip6/data/CMIP6/ScenarioMIP/NCC/NorESM2-MM/ssp585/r1i1p1f1/Oyr/o2/gn/latest/o2_Oyr_NorESM2-MM_ssp585_r1i1p1f1_gn_2015-2020.nc",
    "/badc/cmip6/data/CMIP6/ScenarioMIP/NCC/NorESM2-LM/ssp585/r1i1p1f1/Oyr/o2/gn/latest/o2_Oyr_NorESM2-LM_ssp585_r1i1p1f1_gn_2015-2020.nc",
    "/badc/cmip6/data/CMIP6/ScenarioMIP/MPI-M/MPI-ESM1-2-LR/ssp585/r1i1p1f1/Oyr/o2/gn/latest/o2_Oyr_MPI-ESM1-2-LR_ssp585_r1i1p1f1_gn_2015-2034.nc"]

for fpath in paths:
    print(fpath)    
    q = Dataset(fpath)
    try:
        x = q.variables['o2'].get_var_chunk_cache()
        print(x)
        print(x[0]/x[1])
    except:
        try:
            x = q.variables['dtime'].get_var_chunk_cache()
            print(x)
            print(x[0]/x[1])
        except:
            print('unrecognised')