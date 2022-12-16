import importlib.util
spec = importlib.util.spec_from_file_location("map_archive_path", "/home/users/wcross/kc-intake/kerchunk-tools/kerchunk_tools/utils.py")
funct = importlib.util.module_from_spec(spec)
spec.loader.exec_module(funct)


def test_map_archive_path_success():
    path1 = "/neodc/sentinel3a/data/OLCI/L1_EFR/2018/11/28/"
    end_path = funct.map_archive_path(path1)
    if end_path == "s3://s3-qb141/archive/spot-9898-28/":
        print("path 1 success")
    else:
        print("path 1 fail")
        
        
    path2 = "/neodc/esacci/ocean_colour/data/v5.0-release/geographic/netcdf/chlor_a/monthly/v5.0/2001/ESACCI-OC-L3S-CHLOR_A-MERGED-1M_MONTHLY_4km_GEO_PML_OCx-200112-fv5.0.nc"
    end_path2 = funct.map_archive_path(path2)
    if end_path2 == "s3://s3-qb139/archive/spot-45772-chlor_a/monthly/v5.0/2001/ESACCI-OC-L3S-CHLOR_A-MERGED-1M_MONTHLY_4km_GEO_PML_OCx-200112-fv5.0.nc":
        print("path 2 success")
    else:
        print("path 2 fail")


def test_map_archive_path_failure():
    fail_path="/my/nonsense/data"
    end_fail_path = funct.map_archive_path(fail_path)
    print(end_fail_path)
    if end_fail_path == "path does not exist":
        print("error handelling success")
        print("exception raised")
    else:
        print("failed to raise exception")



test_map_archive_path_success()
test_map_archive_path_failure()


#python map-path.py /neodc/esacci/ocean_colour/data/v5.0-release/geographic/netcdf/chlor_a/monthly/v5.0/2001/ESACCI-OC-L3S-CHLOR_A-MERGED-1M_MONTHLY_4km_GEO_PML_OCx-200112-fv5.0.nc
#[DEBUG] Realpath: /datacentre/archvol5/qb139/archive/spot-45772-chlor_a/monthly/v5.0/2001/ESACCI-OC-L3S-CHLOR_A-MERGED-1M_MONTHLY_4km_GEO_PML_OCx-200112-fv5.0.nc
#s3://s3-qb139/archive/spot-45772-chlor_a/monthly/v5.0/2001/ESACCI-OC-L3S-CHLOR_A-MERGED-1M_MONTHLY_4km_GEO_PML_OCx-200112-fv5.0.nc

#s3cmd ls s3://s3-qb139/archive/spot-45772-chlor_a/monthly/v5.0/2001/ESACCI-OC-L3S-CHLOR_A-MERGED-1M_MONTHLY_4km_GEO_PML_OCx-200112-fv5.0.nc
#2021-01-18 15:49    201457586  s3://s3-qb139/archive/spot-45772-chlor_a/monthly/v5.0/2001/ESACCI-OC-L3S-CHLOR_A-MERGED-1M_MONTHLY_4km_GEO_PML_OCx-200112-fv5.0.nc




