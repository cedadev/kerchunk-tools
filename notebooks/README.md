# ESACCI Datasets
Here is a list of all the currently available ESACCI kerchunked datasets, paths to the kerchunk file and considerations when using the file.
All kerchunk files listed here represent publicly available data and should not require authentication. Thus to open the kerchunk file, you
may use the standard opening sequence as follows:
```
kfile  = 'path/to/kerchunk.json'
mapper = fsspec.get_mapper('reference://', fo=kfile)# add `remote_options=remote_options` for authentication with a certificate
ds     = xr.open_zarr(mapper, consolidated=False, decode_times=False)
```
All kerchunk files are currently accessible via `https://dap.ceda.ac.uk/neodc/esacci/snow/docs/` with the addition of the file name. A naming scheme is in development, but for now please refer to the list below for the correct dataset.
For each dataset there is also an information section with a suggested variable and test map bounds. These 

## Quick Use List
 - Biomass Merged (esacci2.json)
 - Biomass Change (esacci3.json)
 - ATSR2-AATSR Cloud (esacci4.json)
 - AVHRR-AM Cloud (esacci5.json)
 - AVHRR-PM Cloud (esacci6.json)
 - ocean_colour all_products (esacci7.json)
 - ocean_colour chlor_a (esacci8.json)
 - ocean_colour iop (esacci9.json)
 - ocean_colour kd (esacci10.json)
 - ocean_colour rrs (esacci11.json)
 - snow swe Merged (esacci12.json)
 - land_cover_maps v2.0.7 (esacci13.json)
 - land_cover pft v2.0.8 (esacci14.json)
 - lst DAY TERRA_MODIS (esacci15.json)
 - lst NIGHT TERRA_MODIS (esacci16.json)
 - lst NIGHT AQUA_MODIS (esacci17.json)
 - lst DAY AQUA_MODIS (esacci18.json)
 - lst DAY MULTISENSOR (esacci19.json)
 - lst NIGHT MULTISENSOR (esacci20.json)
 - permafrost layer_thickness (esacci21.json)
 - sea_ice_thickness cryosat NH (esacci22.json)
 - sea_ice_thickness cryosat SH (esacci23.json)
 - sea_ice_thickness envisat SH (esacci24.json)
 - sea_ice_thickness envisat NH (esacci25.json)
 - sea_ice_concentration NH (esacci26.json)
 - sea_ice_concentration SH (esacci27.json)
 - fire MODIS (esacci28.json)
 - soil_moisture ACTIVE (esacci29.json)
 - soil_moisture PASSIVE (esacci30.json)
 - soil_moisture COMBINED (esacci31.json)
 - soil_moisture break_adjusted (esacci32.json)

## Biomass Merged (esacci2.json)
 - Represents: `/neodc/esacci/biomass/data/agb/maps/v4.0/netcdf/ESACCI-BIOMASS-L4-AGB-*MERGED*nc`
 - Kerchunk File Size: 469 KB (good)
 - Variables: agb (above-ground biomass)
 - Test Map Bounds: Origin-centred `(25, 0) to (15, 10)` Algeria-Mali-Niger

## Biomass Change (esacci3.json)
 - Represents: `/neodc/esacci/biomass/data/agb/maps/v4.0/netcdf/ESACCI-BIOMASS-L4-AGB-*CHANGE*nc`
 - Kerchunk File Size: 376 KB (good)

## ATSR2-AATSR Cloud (esacci4.json)
 - Represents: `/neodc/esacci/cloud/data/version3/L3C/ATSR2-AATSR/v3.0/*/*/*nc`
 - Kerchunk File Size: 18 MB (average)

## AVHRR-AM Cloud (esacci5.json)
 - Represents: `/neodc/esacci/cloud/data/version3/L3C/AVHRR-AM/v3.0/*/*/*nc`
 - Kerchunk File Size: 27 MB (average)

## AVHRR-PM Cloud (esacci6.json)
 - Represents: `/neodc/esacci/cloud/data/version3/L3C/AVHRR-PM/v3.0/*/*/*nc`
 - Kerchunk File Size: 37 MB (average)

## ocean_colour all_products (esacci7.json)
 - Represents: `/neodc/esacci/ocean_colour/data/v6.0-release/geographic/netcdf/all_products/monthly/v6.0/*/*nc`
 - Kerchunk File Size: 3.4 GB (poor)

## ocean_colour chlor_a (esacci8.json)
 - Represents: `/neodc/esacci/ocean_colour/data/v6.0-release/geographic/netcdf/chlor_a/monthly/v6.0/*/*nc`
 - Kerchunk File Size: 348 MB (poor)

## ocean_colour iop (esacci9.json)
 - Represents: `/neodc/esacci/ocean_colour/data/v6.0-release/geographic/netcdf/iop/monthly/v6.0/*/*nc`
 - Kerchunk File Size: 1.9 GB (poor)

## ocean_colour kd (esacci10.json)
 - Represents: `/neodc/esacci/ocean_colour/data/v6.0-release/geographic/netcdf/kd/monthly/v6.0/*/*nc`
 - Kerchunk File Size: 344 MB (poor)

## ocean_colour rrs (esacci11.json)
 - Represents: `/neodc/esacci/ocean_colour/data/v6.0-release/geographic/netcdf/rrs/monthly/v6.0/*/*nc`
 - Kerchunk File Size: 1.3 GB (poor)

## snow swe Merged (esacci12.json)
 - Represents: `/neodc/esacci/snow/data/swe/MERGED/v2.0/*/*/*nc`
 - Kerchunk File Size: 24 MB (average)

## land_cover_maps v2.0.7 (esacci13.json)
 - Represents: `/neodc/esacci/land_cover/data/land_cover_maps/v2.0.7/*nc`
 - Kerchunk File Size: N/A

## land_cover pft v2.0.8 (esacci14.json)
 - Represents: `/neodc/esacci/land_cover/data/pft/v2.0.8/*nc`
 - Kerchunk File Size: 147 MB (poor)

## lst DAY TERRA_MODIS (esacci15.json)
 - Represents: `/neodc/esacci/land_surface_temperature/data/TERRA_MODIS/L3C/0.01/v3.00/monthly/*/*/*MONTHLY_DAY*.nc`
 - Kerchunk File Size: 411 MB (poor)

## lst NIGHT TERRA_MODIS (esacci16.json)
 - Represents: `/neodc/esacci/land_surface_temperature/data/TERRA_MODIS/L3C/0.01/v3.00/monthly/*/*/*MONTHLY_NIGHT*.nc`
 - Kerchunk File Size: 415 MB (poor)

## lst NIGHT AQUA_MODIS (esacci17.json)
 - Represents: `/neodc/esacci/land_surface_temperature/data/AQUA_MODIS/L3C/0.01/v3.00/monthly/*/*/*MONTHLY_NIGHT*.nc`
 - Kerchunk File Size: 362 MB (poor)

## lst DAY AQUA_MODIS (esacci18.json)
 - Represents: `/neodc/esacci/land_surface_temperature/data/AQUA_MODIS/L3C/0.01/v3.00/monthly/*/*/*MONTHLY_DAY*.nc`
 - Kerchunk File Size: 359 MB (poor)

## lst DAY MULTISENSOR (esacci19.json)
 - Represents: `/neodc/esacci/land_surface_temperature/data/MULTISENSOR_IRCDR/L3S/0.01/v2.00/monthly/*/*/ESACCI-LST-L3S-LST-IRCDR_-0.01deg_1MONTHLY_*DAY*nc`
 - Kerchunk File Size: 550 MB (poor)

## lst NIGHT MULTISENSOR (esacci20.json)
 - Represents: `/neodc/esacci/land_surface_temperature/data/MULTISENSOR_IRCDR/L3S/0.01/v2.00/monthly/*/*/ESACCI-LST-L3S-LST-IRCDR_-0.01deg_1MONTHLY_*NIGHT*nc`
 - Kerchunk File Size: 554 MB (poor)

## permafrost layer_thickness (esacci21.json)
 - Represents: `/neodc/esacci/permafrost/data/active_layer_thickness/L4/area4/pp/v03.0/*nc`
 - Kerchunk File Size: 373 KB (good)

## sea_ice_thickness cryosat NH (esacci22.json)
 - Represents: `/neodc/esacci/sea_ice/data/sea_ice_thickness/L3C/cryosat2/v2.0/NH/*/ESACCI-SEAICE-L3C-SITHICK-SIRAL_CRYOSAT2-NH25KMEASE2-*nc`
 - Kerchunk File Size: 113 KB (good)

## sea_ice_thickness cryosat SH (esacci23.json)
 - Represents: `/neodc/esacci/sea_ice/data/sea_ice_thickness/L3C/cryosat2/v2.0/SH/*/ESACCI-SEAICE-L3C-SITHICK-SIRAL_CRYOSAT2*nc`
 - Kerchunk File Size: 175 KB (good)

## sea_ice_thickness envisat SH (esacci24.json)
 - Represents: `/neodc/esacci/sea_ice/data/sea_ice_thickness/L3C/envisat/v2.0/SH/*/*nc`
 - Kerchunk File Size: 255 KB (good)

## sea_ice_thickness envisat NH (esacci25.json)
 - Represents: `/neodc/esacci/sea_ice/data/sea_ice_thickness/L3C/envisat/v2.0/NH/*/*nc`
 - Kerchunk File Size: 154 KB (good)

## sea_ice_concentration NH (esacci26.json)
 - Represents: `/neodc/esacci/sea_ice/data/sea_ice_concentration/L4/ssmi_ssmis/12.5km/v3.0/NH/*/*/*nc`
 - Kerchunk File Size: 20 MB (average)

## sea_ice_concentration SH (esacci27.json)
 - Represents: `/neodc/esacci/sea_ice/data/sea_ice_concentration/L4/ssmi_ssmis/12.5km/v3.0/SH/*/*/*nc`
 - Kerchunk File Size: 20 MB (average)

## fire MODIS (esacci28.json)
 - Represents: `/neodc/esacci/fire/data/burned_area/MODIS/grid/v5.1/*/*nc`
 - Kerchunk File Size: 377 KB (good)

## soil_moisture ACTIVE (esacci29.json)
 - Represents: `/neodc/esacci/soil_moisture/data/daily_files/ACTIVE/v07.1/*/*nc`
 - Kerchunk File Size: 16 MB (average)

## soil_moisture PASSIVE (esacci30.json)
 - Represents: `/neodc/esacci/soil_moisture/data/daily_files/PASSIVE/v07.1/*/*nc`
 - Kerchunk File Size: 23 MB (average)

## soil_moisture COMBINED (esacci31.json)
 - Represents: `/neodc/esacci/soil_moisture/data/daily_files/COMBINED/v07.1/*/*nc`
 - Kerchunk File Size: 23 MB (average)

## soil_moisture break_adjusted (esacci32.json)
 - Represents: `/neodc/esacci/soil_moisture/data/daily_files/break_adjusted_COMBINED/v07.1/*/*nc`
 - Kerchunk File Size: 23 MB (average)