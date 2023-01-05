import os
import sys

def prepare_dir(dr):
    if not os.path.isdir(dr):
        os.makedirs(dr)




def map_archive_path(pth):
    realpath = os.path.realpath(pth)
    print(f"[DEBUG] Realpath: {realpath}")
    match = "/".join(realpath.split("/")[3:6])
    suffix = "/".join(realpath.split("/")[6:])
    
    end = "/" if pth.endswith("/") else ""
    sep = "/" if suffix else ""
    
    end_path = f"s3://s3-{match}{sep}{suffix}{end}"
    if match=="data" and suffix=="":
        return("path does not exist")
    return end_path

if __name__ == "__main__":
    pth = sys.argv[1]
    print(map_archive_path(pth))