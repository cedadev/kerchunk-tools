import os
#from subprocess import PIPE, Popen

#kerchunk_tools create -p $prefix -o $kc_file -b $max_bytes $filepaths

#prefix = end file destination
#kc_file = name of end file
#max bytes = does nothing?
#filepaths = file paths to make into file?




#kerchunk_tools create -p outputs/kc-indexes/files -o CMIP6.CMIP.CSIRO.ACCESS-ESM1-5.historical.r1i1p1f1.Oyr.o2.gn.latest.json -b 50000 /badc/cmip6/data/CMIP6/CMIP/CSIRO/ACCESS-ESM1-5/historical/r1i1p1f1/Oyr/o2/gn/latest/o2_Oyr_ACCESS-ESM1-5_historical_r1i1p1f1_gn_1850-1949.nc
#^^works^^



#kerchunk_tools create -p outputs/kc-indexes/files -o CMIP6.CMIP.CSIRO.ACCESS-ESM1-5.historical.r1i1p1f1.Oyr.o2.gn.latest.json -b 50000 /badc/cmip6/data/CMIP6/CMIP/CSIRO/ACCESS-ESM1-5/historical/r1i1p1f1/Oyr/o2/gn/latest/o2_Oyr_ACCESS-ESM1-5_historical_r1i1p1f1_gn_1850-1949.nc /badc/cmip6/data/CMIP6/CMIP/CSIRO/ACCESS-ESM1-5/historical/r1i1p1f1/Oyr/o2/gn/latest/o2_Oyr_ACCESS-ESM1-5_historical_r1i1p1f1_gn_1850-1949.nc /badc/cmip6/data/CMIP6/CMIP/CSIRO/ACCESS-ESM1-5/historical/r1i1p1f1/Oyr/o2/gn/latest/o2_Oyr_ACCESS-ESM1-5_historical_r1i1p1f1_gn_1950-2014.nc




def test_cli_help_option_success():
    stream = os.popen('kerchunk_tools create --help')
    output = stream.read()
    print(output)
    
    
    thing = """Usage: kerchunk_tools create [OPTIONS] [FILE_URIS]...

  Create a Kerchunk index file and save to POSIX/object-store. If multiple
  file_uris provided then aggregate them.

Options:
  -f, --file-uris-file TEXT
  -p, --prefix TEXT
  -o, --output-path TEXT
  -b, --max-bytes INTEGER
  -c, --s3-config-file TEXT
  -C, --cache_dir TEXT
  --help                     Show this message and exit.
"""

    assert output == thing, f"incorrect response for kerchunk_tools create --help   got: {output}"
    


def test_cli_single_path_success():
    stream = os.popen('kerchunk_tools create -p outputs/kc-indexes/files -o CMIP6.CMIP.CSIRO.ACCESS-ESM1-5.historical.r1i1p1f1.Oyr.o2.gn.latest.json -b 50000 /badc/cmip6/data/CMIP6/CMIP/CSIRO/ACCESS-ESM1-5/historical/r1i1p1f1/Oyr/o2/gn/latest/o2_Oyr_ACCESS-ESM1-5_historical_r1i1p1f1_gn_1850-1949.nc')
    output = stream.read()
    print(output, "OUTPUT TEXT #################")
    thing = """[INFO] Processing: /badc/cmip6/data/CMIP6/CMIP/CSIRO/ACCESS-ESM1-5/historical/r1i1p1f1/Oyr/o2/gn/latest/o2_Oyr_ACCESS-ESM1-5_historical_r1i1p1f1_gn_1850-1949.nc
[INFO] Written file: outputs/kc-indexes/files/CMIP6.CMIP.CSIRO.ACCESS-ESM1-5.historical.r1i1p1f1.Oyr.o2.gn.latest.json
"""
    assert output == thing, f"incorrect response for single path data   got: {output}"



def test_cli_multiple_file_success():
    stream = os.popen('kerchunk_tools create -p outputs/kc-indexes/files -o CMIP6.CMIP.CSIRO.ACCESS-ESM1-5.historical.r1i1p1f1.Oyr.o2.gn.latest.json -b 50000 /badc/cmip6/data/CMIP6/CMIP/CSIRO/ACCESS-ESM1-5/historical/r1i1p1f1/Oyr/o2/gn/latest/o2_Oyr_ACCESS-ESM1-5_historical_r1i1p1f1_gn_1850-1949.nc /badc/cmip6/data/CMIP6/CMIP/CSIRO/ACCESS-ESM1-5/historical/r1i1p1f1/Oyr/o2/gn/latest/o2_Oyr_ACCESS-ESM1-5_historical_r1i1p1f1_gn_1850-1949.nc /badc/cmip6/data/CMIP6/CMIP/CSIRO/ACCESS-ESM1-5/historical/r1i1p1f1/Oyr/o2/gn/latest/o2_Oyr_ACCESS-ESM1-5_historical_r1i1p1f1_gn_1950-2014.nc')
    output = stream.read()
    print(output, "OUTPUT TEXT #################")
    thing = """[INFO] Processing: /badc/cmip6/data/CMIP6/CMIP/CSIRO/ACCESS-ESM1-5/historical/r1i1p1f1/Oyr/o2/gn/latest/o2_Oyr_ACCESS-ESM1-5_historical_r1i1p1f1_gn_1850-1949.nc
[INFO] Processing: /badc/cmip6/data/CMIP6/CMIP/CSIRO/ACCESS-ESM1-5/historical/r1i1p1f1/Oyr/o2/gn/latest/o2_Oyr_ACCESS-ESM1-5_historical_r1i1p1f1_gn_1850-1949.nc
[INFO] Processing: /badc/cmip6/data/CMIP6/CMIP/CSIRO/ACCESS-ESM1-5/historical/r1i1p1f1/Oyr/o2/gn/latest/o2_Oyr_ACCESS-ESM1-5_historical_r1i1p1f1_gn_1950-2014.nc
[INFO] Written file: outputs/kc-indexes/files/CMIP6.CMIP.CSIRO.ACCESS-ESM1-5.historical.r1i1p1f1.Oyr.o2.gn.latest.json
"""
    assert output == thing, f"incorrect response for multiple path data   got: {output}"


def test_cli_correct_file_name():
	stream = os.popen('kerchunk_tools create -p outputs/kc-indexes/files -o correct_file_name -b 50000 /badc/cmip6/data/CMIP6/CMIP/CSIRO/ACCESS-ESM1-5/historical/r1i1p1f1/Oyr/o2/gn/latest/o2_Oyr_ACCESS-ESM1-5_historical_r1i1p1f1_gn_1850-1949.nc')
	output = stream.read()
	thing = "correct_file_name"
	output = (output.splitlines()[1])
	output = output.split()
	output = output[-1]
	output = output.split("/")
	output = output[-1]
	print(output, "OUTPUT TEXT #################")
	
	assert output == thing, f"incorrect filename used   got: {output}"





def test_cli_single_path_failure():
    try:
        stream = os.popen('kerchunk_tools create -p outputs/kc-indexes/files -o CMIP6.CMIP.CSIRO.ACCESS-ESM1-5.historical.r1i1p1f1.Oyr.o2.gn.latest.json -b 50000 random/place')
        output = stream.read()
        print(f"input directory should not exist but does {output}")
    except FileNotFoundError:
        return()
    


def test_cli_correct_prefix():
    stream = os.popen('kerchunk_tools create -p outputs/kc-indexes/files -o CMIP6.CMIP.CSIRO.ACCESS-ESM1-5.historical.r1i1p1f1.Oyr.o2.gn.latest.json -b 50000 /badc/cmip6/data/CMIP6/CMIP/CSIRO/ACCESS-ESM1-5/historical/r1i1p1f1/Oyr/o2/gn/latest/o2_Oyr_ACCESS-ESM1-5_historical_r1i1p1f1_gn_1850-1949.nc')
    output = stream.read()
    output = (output.splitlines()[1])
    output = output.split()
    output = output[-1]
    output = output.split("/")
    del output[-1]
    output = "/".join(output)
    print(output, "OUTPUT TEXT #################")
    thing = "outputs/kc-indexes/files"
    assert output == thing, f"incorrect response for single path data   got: {output}"



def test_cli_files_uris_file_works():
    stream = os.popen('kerchunk_tools create -f pathlist.txt -p outputs/kc-indexes/files -o TEST.CMIP6.CMIP.CSIRO.ACCESS-ESM1-5.historical.r1i1p1f1.Oyr.o2.gn.latest.json -b 50000')
    output = stream.read()
    print(output, "OUTPUT TEXT #################")
    thing = """[INFO] Processing: /badc/cmip6/data/CMIP6/CMIP/CSIRO/ACCESS-ESM1-5/historical/r1i1p1f1/Oyr/o2/gn/latest/o2_Oyr_ACCESS-ESM1-5_historical_r1i1p1f1_gn_1850-1949.nc
[INFO] Processing: /badc/cmip6/data/CMIP6/CMIP/CSIRO/ACCESS-ESM1-5/historical/r1i1p1f1/Oyr/o2/gn/latest/o2_Oyr_ACCESS-ESM1-5_historical_r1i1p1f1_gn_1850-1949.nc
[INFO] Processing: /badc/cmip6/data/CMIP6/CMIP/CSIRO/ACCESS-ESM1-5/historical/r1i1p1f1/Oyr/o2/gn/latest/o2_Oyr_ACCESS-ESM1-5_historical_r1i1p1f1_gn_1950-2014.nc
[INFO] Written file: outputs/kc-indexes/files/TEST.CMIP6.CMIP.CSIRO.ACCESS-ESM1-5.historical.r1i1p1f1.Oyr.o2.gn.latest.json
"""
    assert output == thing, f"incorrect response for multiple path data using a file to get the information   got: {output}"
    

def test_map_path_success():
    stream = os.popen('kerchunk_tools map /neodc/sentinel3a/data/OLCI/L1_EFR/2018/11/28/')
    output = stream.read()
    print(output, "OUTPUT TEXT #################")
    thing="""s3://s3-qb141/archive/spot-9898-28/
"""
    assert output == thing, f"Response incorrect from map_archive_path with short path and with /: {output}"





#create:

#help						done
#single path				done
#multi path					done
#not enough bytes           ????
#correct file name			done
#wrong prefix               done
#bad input path             done
#-f works                   done


#map:

#do thing



#prefix = end file destination
#kc_file = name of end file
#max bytes = does nothing?
#filepaths = file paths to make into file
#(-f) files-uris-file = provide a text file that includes a list of file URIs (with one line per URI)
#(-c) s3-config-file = points to an S3 configuration file (when you are writing to S3 object store)
