import os


def prepare_dir(dr):
    if not os.path.isdir(dr):
        os.makedirs(dr)


