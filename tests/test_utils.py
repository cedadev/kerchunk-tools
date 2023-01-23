import pytest
from kerchunk_tools import utils
    
    
def test_map_archive_path_success_short_with():
    resp = utils.map_archive_path("/neodc/sentinel3a/data/OLCI/L1_EFR/2018/11/28/")
    assert resp == "s3://s3-qb141/archive/spot-9898-28/", f"Response incorrect from map_archive_path with short path and with /: {resp}"

def test_map_archive_path_success_long():
    resp = utils.map_archive_path("/neodc/esacci/ocean_colour/data/v5.0-release/geographic/netcdf/chlor_a/monthly/v5.0/2001/ESACCI-OC-L3S-CHLOR_A-MERGED-1M_MONTHLY_4km_GEO_PML_OCx-200112-fv5.0.nc")
    assert resp == "s3://s3-qb139/archive/spot-45772-chlor_a/monthly/v5.0/2001/ESACCI-OC-L3S-CHLOR_A-MERGED-1M_MONTHLY_4km_GEO_PML_OCx-200112-fv5.0.nc", f"Response incorrect from map_archive_path with long path: {resp}"
    
    
def test_map_archive_path_success_short_without():
    resp = utils.map_archive_path("/neodc/esacci/ocean_colour/data/v5.0-release/geographic/netcdf/chlor_a")
    assert resp == "s3://s3-qb139/archive/spot-45772-chlor_a", f"Response incorrect from map_archive_path with short path and no /: {resp}"
    
    
def test_map_archive_path_failure():
    pth = "/my/nonsense/data"
    expected = f"Path does not exist: {pth}"

    with pytest.raises(Exception) as excinfo:
        resp = utils.map_archive_path(pth)

    assert str(excinfo.value) == expected, f"Response incorrect from map_archive_path with incorrect data (should be '{expected}'), but is: {resp}"
