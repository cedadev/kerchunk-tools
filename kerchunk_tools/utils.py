import os
import sys


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

