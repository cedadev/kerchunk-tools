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
For each dataset there is also an information section with a test variable and test map bounds. These are determined such that the chunk size will give an array of roughly 1GB in size. Coordinates are shown as a bounding box, (top-left) to (bottom-right)

## Quick Use List
 - Biomass Merged (esacci2.json)
 - Biomass Change (esacci3.json)
 - ATSR2-AATSR Cloud (esacci4.json)
 - AVHRR-AM Cloud (esacci5.json)
 - AVHRR-PM Cloud (esacci6.json)
 - ocean_colour all_products (esacci7.json)*
 - ocean_colour chlor_a (esacci8.json)
 - ocean_colour iop (esacci9.json)
 - ocean_colour kd (esacci10.json)
 - ocean_colour rrs (esacci11.json)
 - snow swe Merged (esacci12.json)
 - land_cover_maps v2.0.7 (esacci13.json)*
 - land_cover pft v2.0.8 (esacci14.json)*
 - lst DAY TERRA_MODIS (esacci15.json)
 - lst NIGHT TERRA_MODIS (esacci16.json)
 - lst NIGHT AQUA_MODIS (esacci17.json)
 - lst DAY AQUA_MODIS (esacci18.json)
 - lst DAY MULTISENSOR (esacci19.json)
 - lst NIGHT MULTISENSOR (esacci20.json)
 - permafrost layer_thickness (esacci21.json)*
 - sea_ice_thickness cryosat NH (esacci22.json)*
 - sea_ice_thickness cryosat SH (esacci23.json)*
 - sea_ice_thickness envisat SH (esacci24.json)*
 - sea_ice_thickness envisat NH (esacci25.json)*
 - sea_ice_concentration NH (esacci26.json)*
 - sea_ice_concentration SH (esacci27.json)*
 - fire MODIS (esacci28.json)
 - soil_moisture ACTIVE (esacci29.json)
 - soil_moisture PASSIVE (esacci30.json)
 - soil_moisture COMBINED (esacci31.json)
 - soil_moisture break_adjusted (esacci32.json)

*: Adjustments are required to the example notebook for these kerchunk files

## Kerchunk Files

### Biomass Merged (esacci2.json)
 - Represents: `/neodc/esacci/biomass/data/agb/maps/v4.0/netcdf/ESACCI-BIOMASS-L4-AGB-*MERGED*nc`
 - Kerchunk File Size: 469 KB (good)
 - Test Variable: agb (above-ground biomass)
 - Test Map Bounds: `(25, 0) to (15, 10) mean all timesteps` Central Africa

### Biomass Change (esacci3.json)
 - Represents: `/neodc/esacci/biomass/data/agb/maps/v4.0/netcdf/ESACCI-BIOMASS-L4-AGB-*CHANGE*nc`
 - Kerchunk File Size: 376 KB (good)
 - Test Variable: diff_qf (above-ground biomass change quality flag)
 - Test Map Bounds: `(20.5, 18.5) to (18.5, 20.5) mean all timesteps` Central Africa

### ATSR2-AATSR Cloud (esacci4.json)
 - Represents: `/neodc/esacci/cloud/data/version3/L3C/ATSR2-AATSR/v3.0/*/*/*nc`
 - Kerchunk File Size: 18 MB (average)
 - Test Variable: boa_lwdn (bottom of atmosphere downwelling thermal radiation)
 - Test Map Bounds: `(90, 180) to (-90, -180) mean first 10 timesteps` Full Map

### AVHRR-AM Cloud (esacci5.json)
 - Represents: `/neodc/esacci/cloud/data/version3/L3C/AVHRR-AM/v3.0/*/*/*nc`
 - Kerchunk File Size: 27 MB (average)
 - Test Variable: boa_lwdn (bottom of atmosphere downwelling thermal radiation)
 - Test Map Bounds: `(90, 180) to (-90, -180) mean first 10 timesteps` Full Map

### AVHRR-PM Cloud (esacci6.json)
 - Represents: `/neodc/esacci/cloud/data/version3/L3C/AVHRR-PM/v3.0/*/*/*nc`
 - Kerchunk File Size: 37 MB (average)
 - Test Variable: boa_lwdn (bottom of atmosphere downwelling thermal radiation)
 - Test Map Bounds: `(90, 180) to (-90, -180) mean first 10 timesteps` Full Map

### ocean_colour all_products (esacci7.json)
 - Represents: `/neodc/esacci/ocean_colour/data/v6.0-release/geographic/netcdf/all_products/monthly/v6.0/*/*nc`
 - Kerchunk File Size: 3.4 GB (poor)
 - Test Variable: chlor_a (Chlorophyll-a concentration in seawater)
 - Test Map Bounds: `(5.2, -4.2) to (-4.2, 5.2) mean first 10 timesteps` Origin Center
 - Note: Extreme loading time for kerchunk file - large size

### ocean_colour chlor_a (esacci8.json)
 - Represents: `/neodc/esacci/ocean_colour/data/v6.0-release/geographic/netcdf/chlor_a/monthly/v6.0/*/*nc`
 - Kerchunk File Size: 348 MB (poor)
 - Test Variable: chlor_a (Chlorophyll-a concentration in seawater)
 - Test Map Bounds: `(5.2, -4.2) to (-4.2, 5.2) mean first 10 timesteps` Ocean

### ocean_colour iop (esacci9.json)
 - Represents: `/neodc/esacci/ocean_colour/data/v6.0-release/geographic/netcdf/iop/monthly/v6.0/*/*nc`
 - Kerchunk File Size: 1.9 GB (poor)
 - Test Variable: atot_412 (Total absorption coefficient at 412 nm)
 - Test Map Bounds: `(5.2, -4.2) to (-4.2, 5.2) mean first 10 timesteps` Ocean

### ocean_colour kd (esacci10.json)
 - Represents: `/neodc/esacci/ocean_colour/data/v6.0-release/geographic/netcdf/kd/monthly/v6.0/*/*nc`
 - Kerchunk File Size: 344 MB (poor)
 - Test Variable: kd_490 (Downwelling attenuation coefficient at 490nm)
 - Test Map Bounds: `(5.2, -4.2) to (-4.2, 5.2) mean first 10 timesteps` Ocean

### ocean_colour rrs (esacci11.json)
 - Represents: `/neodc/esacci/ocean_colour/data/v6.0-release/geographic/netcdf/rrs/monthly/v6.0/*/*nc`
 - Kerchunk File Size: 1.3 GB (poor)
 - Test Variable: Rrs_412 (Sea surface reflectance at 412 nm)
 - Test Map Bounds: `(5.2, -4.2) to (-4.2, 5.2) mean first 10 timesteps` Ocean

### snow swe Merged (esacci12.json)
 - Represents: `/neodc/esacci/snow/data/swe/MERGED/v2.0/*/*/*nc`
 - Kerchunk File Size: 24 MB (average)
 - Test Variable: swe (Snow Water Equivalent)
 - Test Map Bounds: `(50, -5) to (40, 5) mean first 10 timesteps` France

### land_cover_maps v2.0.7 (esacci13.json)
 - Represents: `/neodc/esacci/land_cover/data/land_cover_maps/v2.0.7/*nc`
 - Kerchunk File Size: 36 MB (average)
 - Test Variable: lccs_class (Land cover class defined in LCCS)
 - Test Map Bounds: `(-51, 7) to (-59, -15) [lat dim inverted]` UK
 - Note: The time dimension that exists in this file is purely for stacking of timesteps, no actual time information is represented. Also, the latitude dimension is in this case inverted, so the array coordinates `[0,0]` correspond not to `[-90, -180] (SW)` but instead to `[90, -180] (NW)`

### land_cover pft v2.0.8 (esacci14.json)
 - Represents: `/neodc/esacci/land_cover/data/pft/v2.0.8/*nc`
 - Kerchunk File Size: 147 MB (poor)
 - Test Variable: SNOWICE (Permanent Snow and Ice)
 - Test Map Bounds: `(-70, -15) to (-80, -30) [lat dim inverted]` Greenland East
 - Note: The latitude dimension is in this case inverted, so the array coordinates `[0,0]` correspond not to `[-90, -180] (SW)` but instead to `[90, -180] (NW)`

### lst DAY TERRA_MODIS (esacci15.json)
 - Represents: `/neodc/esacci/land_surface_temperature/data/TERRA_MODIS/L3C/0.01/v3.00/monthly/*/*/*MONTHLY_DAY*.nc`
 - Kerchunk File Size: 411 MB (poor)
 - Test Variable: lst (Land Surface Temperature)
 - Test Map Bounds: `(59, 7) to (51, -15)` UK

### lst NIGHT TERRA_MODIS (esacci16.json)
 - Represents: `/neodc/esacci/land_surface_temperature/data/TERRA_MODIS/L3C/0.01/v3.00/monthly/*/*/*MONTHLY_NIGHT*.nc`
 - Kerchunk File Size: 415 MB (poor)
 - Test Variable: lst (Land Surface Temperature)
 - Test Map Bounds: `(59, 7) to (51, -15)` UK

### lst NIGHT AQUA_MODIS (esacci17.json)
 - Represents: `/neodc/esacci/land_surface_temperature/data/AQUA_MODIS/L3C/0.01/v3.00/monthly/*/*/*MONTHLY_NIGHT*.nc`
 - Kerchunk File Size: 362 MB (poor)
 - Test Variable: lst (Land Surface Temperature)
 - Test Map Bounds: `(59, 7) to (51, -15)` UK

### lst DAY AQUA_MODIS (esacci18.json)
 - Represents: `/neodc/esacci/land_surface_temperature/data/AQUA_MODIS/L3C/0.01/v3.00/monthly/*/*/*MONTHLY_DAY*.nc`
 - Kerchunk File Size: 359 MB (poor)
 - Test Variable: lst (Land Surface Temperature)
 - Test Map Bounds: `(59, 7) to (51, -15)` UK

### lst DAY MULTISENSOR (esacci19.json)
 - Represents: `/neodc/esacci/land_surface_temperature/data/MULTISENSOR_IRCDR/L3S/0.01/v2.00/monthly/*/*/ESACCI-LST-L3S-LST-IRCDR_-0.01deg_1MONTHLY_*DAY*nc`
 - Kerchunk File Size: 550 MB (poor)
 - Test Variable: lst (Land Surface Temperature)
 - Test Map Bounds: `(59, 7) to (51, -15)` UK

### lst NIGHT MULTISENSOR (esacci20.json)
 - Represents: `/neodc/esacci/land_surface_temperature/data/MULTISENSOR_IRCDR/L3S/0.01/v2.00/monthly/*/*/ESACCI-LST-L3S-LST-IRCDR_-0.01deg_1MONTHLY_*NIGHT*nc`
 - Kerchunk File Size: 554 MB (poor)
 - Test Variable: lst (Land Surface Temperature)
 - Test Map Bounds: `(59, 7) to (51, -15)` UK

### permafrost layer_thickness (esacci21.json)
 - Represents: `/neodc/esacci/permafrost/data/active_layer_thickness/L4/area4/pp/v03.0/*nc`
 - Kerchunk File Size: 373 KB (good)
 - No spatial plot, lat and lon dimensions replaced with x and y of projection

### sea_ice_thickness cryosat NH (esacci22.json)
 - Represents: `/neodc/esacci/sea_ice/data/sea_ice_thickness/L3C/cryosat2/v2.0/NH/*/ESACCI-SEAICE-L3C-SITHICK-SIRAL_CRYOSAT2-NH25KMEASE2-*nc`
 - Kerchunk File Size: 113 KB (good)
 - Test Variable: sea_ice_thickness (Sea Ice Thickness)
 - Lat and lon coordinates replaced by yc, xc coordinates from centre of frame. (Antarctic Region)

### sea_ice_thickness cryosat SH (esacci23.json)
 - Represents: `/neodc/esacci/sea_ice/data/sea_ice_thickness/L3C/cryosat2/v2.0/SH/*/ESACCI-SEAICE-L3C-SITHICK-SIRAL_CRYOSAT2*nc`
 - Kerchunk File Size: 175 KB (good)
 - Test Variable: sea_ice_thickness (Sea Ice Thickness)
 - Lat and lon coordinates replaced by yc, xc coordinates from centre of frame. (Antarctic Region)

### sea_ice_thickness envisat SH (esacci24.json)
 - Represents: `/neodc/esacci/sea_ice/data/sea_ice_thickness/L3C/envisat/v2.0/SH/*/*nc`
 - Kerchunk File Size: 255 KB (good)
 - Test Variable: sea_ice_thickness (Sea Ice Thickness)
 - Lat and lon coordinates replaced by yc, xc coordinates from centre of frame. (Antarctic Region)

### sea_ice_thickness envisat NH (esacci25.json)
 - Represents: `/neodc/esacci/sea_ice/data/sea_ice_thickness/L3C/envisat/v2.0/NH/*/*nc`
 - Kerchunk File Size: 154 KB (good)
 - Test Variable: sea_ice_thickness (Sea Ice Thickness)
 - Lat and lon coordinates replaced by yc, xc coordinates from centre of frame. (Antarctic Region)

### sea_ice_concentration NH (esacci26.json)
 - Represents: `/neodc/esacci/sea_ice/data/sea_ice_concentration/L4/ssmi_ssmis/12.5km/v3.0/NH/*/*/*nc`
 - Kerchunk File Size: 20 MB (average)
 - Test Variable: ice_conc (Sea Ice Concentration)
 - Lat and lon coordinates replaced by yc, xc coordinates from centre of frame. (Polar Region)

### sea_ice_concentration SH (esacci27.json)
 - Represents: `/neodc/esacci/sea_ice/data/sea_ice_concentration/L4/ssmi_ssmis/12.5km/v3.0/SH/*/*/*nc`
 - Kerchunk File Size: 20 MB (average)
 - Test Variable: ice_conc (Sea Ice Concentration)
 - Lat and lon coordinates replaced by yc, xc coordinates from centre of frame. (Polar Region)

### fire MODIS (esacci28.json)
 - Represents: `/neodc/esacci/fire/data/burned_area/MODIS/grid/v5.1/*/*nc`
 - Kerchunk File Size: 377 KB (good)
 - Test Variable: burned_area
 - Test Map Bounds: `(20, 10) to (10, 20)` Ghana

### soil_moisture ACTIVE (esacci29.json)
 - Represents: `/neodc/esacci/soil_moisture/data/daily_files/ACTIVE/v07.1/*/*nc`
 - Kerchunk File Size: 16 MB (average)
 - Test Variable: sm
 - Test Map Bounds: `(90, 180) to (-90, -180)` Full Map (100 timesteps mean)

### soil_moisture PASSIVE (esacci30.json)
 - Represents: `/neodc/esacci/soil_moisture/data/daily_files/PASSIVE/v07.1/*/*nc`
 - Kerchunk File Size: 23 MB (average)
 - Test Variable: sm
 - Test Map Bounds: `(90, 180) to (-90, -180)` Full Map (100 timesteps mean)

### soil_moisture COMBINED (esacci31.json)
 - Represents: `/neodc/esacci/soil_moisture/data/daily_files/COMBINED/v07.1/*/*nc`
 - Kerchunk File Size: 23 MB (average)
 - Test Variable: sm
 - Test Map Bounds: `(90, 180) to (-90, -180)` Full Map (100 timesteps mean)

### soil_moisture break_adjusted (esacci32.json)
 - Represents: `/neodc/esacci/soil_moisture/data/daily_files/break_adjusted_COMBINED/v07.1/*/*nc`
 - Kerchunk File Size: 23 MB (average)
 - Test Variable: sm
 - Test Map Bounds: `(90, 180) to (-90, -180)` Full Map (100 timesteps mean)

## Frequent Errors

### TypeError: Image data of dtype timedelta64[ns] cannot be converted to float
You need to add `decode_times=False` to `xr.open_zarr` statement.

### ValueError: not enough values to unpack (expected 3, got 0)
You've sliced your array so one or more dimensions is squeezed to zero, consider changing map/data bounds

### AttributeError: 'Dataset' object has no attribute 'lat'
The netcdf files do not have a latitude component, but will likely have an alternative labelled with something else (commonly y or yc)