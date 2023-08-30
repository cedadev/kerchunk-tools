"""Console script for kerchunk_tools."""

__author__ = """Ag Stephens"""
__contact__ = 'ag.stephens@stfc.ac.uk'
__copyright__ = "Copyright 2022 United Kingdom Research and Innovation"
__license__ = "BSD - see LICENSE file in top-level package directory"


<<<<<<< HEAD
from .indexer import Indexer, get_default
from .xarray_wrapper import wrap_xr_open
from .utils import map_archive_path
=======
from indexer import Indexer
from xarray_wrapper import wrap_xr_open
from utils import map_archive_path
>>>>>>> main

import sys
import json
import click

@click.group()
def main():
    """Console script for kerchunk-tools."""
    return 0


def parse_s3_config_file(fpath):
    return json.load(open(fpath))


@main.command()
@click.argument("file_uris", nargs=-1)
@click.option("-f", "--file-uris-file", default=None)
@click.option("-p", "--prefix", default=get_default("prefix"))
@click.option("-o", "--output-path", default=get_default("output_path"))
@click.option("-b", "--max-bytes", default=get_default("max_bytes"))
@click.option("-e", "--engine", default=None)
@click.option("-s", "--s3-config-file", default=None)
@click.option("-S", "--scheme", default=None)
@click.option("-d", "--concat-dims", default=None)
@click.option("-i", "--identical-dims", default=None)
@click.option("-c", "--compression", default=None)
@click.option("-C", "--cache_dir", default=None)
@click.option('-t', "--add_time", default=None)
def create(file_uris, file_uris_file=None, prefix=get_default("prefix"), 
           output_path=get_default("output_path"), max_bytes=get_default("max_bytes"), engine=None,
           s3_config_file=None, scheme=None, concat_dims=None, identical_dims=None, compression=None, 
           cache_dir=None, add_time=None):
    """
    Create a Kerchunk index file and save to POSIX/object-store. If multiple
    file_uris provided then aggregate them.
    """
    s3_config = parse_s3_config_file(s3_config_file) if s3_config_file else None
    if file_uris and file_uris_file:
        raise ValueError(f"Tool does not support setting BOTH 'file_uris' and 'file_uris_file' options")

    file_uris = open(file_uris_file).read().strip().split() if file_uris_file else file_uris
    identical_dims = identical_dims.split(",") if identical_dims else None
    concat_dims = concat_dims.split(",") if concat_dims else None

    indexer = Indexer(s3_config=s3_config, scheme=scheme, max_bytes=max_bytes, cache_dir=cache_dir)
    indexer.create(file_uris, prefix, output_path=output_path, identical_dims=identical_dims,
                   concat_dims=concat_dims, compression=compression, engine=engine, max_bytes=max_bytes,
                   add_time=add_time)


@main.command()
@click.argument("filepath") 
def map(filepath):
    """
    Map a file path to a file URI in S3.
    Print the mapped path to stdout.
    """ 
    new_path = map_archive_path(filepath)
    print(new_path)


if __name__ == "__main__":
    sys.exit(main())  # pragma: no cover

