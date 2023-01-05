
# def test_map_archive_path_success():
#     print("-"*20)
    
#     path1 = "/neodc/sentinel3a/data/OLCI/L1_EFR/2018/11/28/"
#     end_path = funct.map_archive_path(path1)
#     if end_path == "s3://s3-qb141/archive/spot-9898-28/":
#         print("path 1 success")
#     else:
#         print("path 1 fail")
        
#     print("-"*20)
        
#     path2 = "/neodc/esacci/ocean_colour/data/v5.0-release/geographic/netcdf/chlor_a/monthly/v5.0/2001/ESACCI-OC-L3S-CHLOR_A-MERGED-1M_MONTHLY_4km_GEO_PML_OCx-200112-fv5.0.nc"
#     end_path2 = funct.map_archive_path(path2)
#     if end_path2 == "s3://s3-qb139/archive/spot-45772-chlor_a/monthly/v5.0/2001/ESACCI-OC-L3S-CHLOR_A-MERGED-1M_MONTHLY_4km_GEO_PML_OCx-200112-fv5.0.nc":
#         print("path 2 success")
#     else:
#         print("path 2 fail")
        
#     print("-"*20)
        
#     path3 = "/neodc/esacci/ocean_colour/data/v5.0-release/geographic/netcdf/chlor_a"
#     end_path3 = funct.map_archive_path(path3)
#     if end_path3 == "s3://s3-qb139/archive/spot-45772-chlor_a":
#         print("path 3 success")
#     else:
#         print("path 3 fail")
        
#     print("-"*20)
    
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
    resp = utils.map_archive_path("/my/nonsense/data")
    assert resp == "path does not exist", f"Response incorrect from map_archive_path with incorrect data (should be path does not exist), but is: {resp}"


# def test_map_archive_path_failure():
#     fail_path="/my/nonsense/data"
#     end_fail_path = funct.map_archive_path(fail_path)
#     print(end_fail_path)
#     if end_fail_path == "path does not exist":
#         print("error handelling success")
#         print("exception raised")
#     else:
#         print("failed to raise exception")

#     print("-"*20)


#test_map_archive_path_success()
#test_map_archive_path_failure()





#import os
#import pytest

#import kerchunk_tools.utils as ktutils

#tests_dir = "tests/dummydir"



# Usage: pytest -v [name of tests]
# E.g.:
# pytest -v tests
# pytest tests/test_utils.py
# Or match name of tests specifically
# pytest -k test_say_name_failures




# def setup_module():
#     "Pytest will find this function by name and automatically run it before tests."
#     if os.path.isdir(tests_dir):
#         os.rmdir(tests_dir)


# def test_prepare_dir_make_new():
#     ktutils.prepare_dir(tests_dir)
#     assert os.path.isdir(tests_dir), f"Failed to create new dir: {tests_dir}"


# def test_prepare_dir_already_exists():
#     ktutils.prepare_dir(tests_dir)
#     assert os.path.isdir(tests_dir), f"Directory not present when already created: {tests_dir}"


# def test_say_name_no_arg():
#     resp = ktutils.say_name()
#     assert resp == "HI Will", f"Response incorrect from say name with no args: {resp}"
    

# def test_say_name_with_name():
#     resp = ktutils.say_name("Ag")
#     assert resp == "HI Ag", f"Response incorrect from say name with name: {resp}"


# def test_say_name_failures():
#     with pytest.raises(TypeError) as exc:
#         ktutils.say_name(12)
#         assert exc.value == 'can only concatenate str (not "int") to str'


# def teardown_module():
#     "Pytest will find this function by name and automatically run it after tests."
#     if os.path.isdir(tests_dir):
#         os.rmdir(tests_dir)