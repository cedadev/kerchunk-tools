import os
import sys
from datetime import datetime

def find_time(time):
    time = time.replace('\n','')
    if len(time) == 4:
        return datetime.strptime(time, "%Y")
    elif len(time) == 6:
        return datetime.strptime(time, "%Y%m")
    else:
        print('Unsupported time type, use YYYY or YYYYMM where possible')
        return None

def prepare_dir(dr):
    if not os.path.isdir(dr):
        os.makedirs(dr)


def map_archive_path(pth):
    if not os.path.exists(pth):
        raise Exception(f"Path does not exist: {pth}")

    realpath = os.path.realpath(pth)

    match = "/".join(realpath.split("/")[3:6])
    suffix = "/".join(realpath.split("/")[6:])
    
    end = "/" if pth.endswith("/") else ""
    sep = "/" if suffix else ""
    
    end_path = f"s3://s3-{match}{sep}{suffix}{end}"
    return end_path


