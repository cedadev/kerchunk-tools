import os
import sys

def prepare_dir(dr):
    if not os.path.isdir(dr):
        os.makedirs(dr)




def map_archive_path(pth):
    realpath = os.path.realpath(pth)
    print(f"[DEBUG] Realpath: {realpath}")
    match = "/".join(realpath.split("/")[3:6])
    match=match+"/"
    suffix = "/".join(realpath.split("/")[6:])
    end_path = f"s3://s3-{match}{suffix}"
    if match=="data/" and suffix=="":
        return("path does not exist")
    return end_path

if __name__ == "__main__":
    pth = sys.argv[1]
    print(map_archive_path(pth))