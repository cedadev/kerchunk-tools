"""Console script for kerchunk_tools."""

__author__ = """Ag Stephens"""
__contact__ = 'ag.stephens@stfc.ac.uk'
__copyright__ = "Copyright 2022 United Kingdom Research and Innovation"
__license__ = "BSD - see LICENSE file in top-level package directory"

import sys
import json
import click

import kerchunk_tools as kct

DEFAULTS = {
    "prefix": "kc-indexes",
    "output_path": "index.json",
    "max_bytes": 10000
}


@click.group()
def main():
    """Console script for kerchunk-tools."""
    return 0


"""
@main.command()
@click.argument("file_path")
@click.option("-m", "--mappings", default=None)
@click.option("-r", "--rules", default=None)
@click.option("-i", "--ignore-attrs", default=None)
@click.option("-G", "--ignore-all-globals", default=False)
@click.option("-D", "--ignore-all-dimensions", default=False)
@click.option("-V", "--ignore-all-variables", default=False)
@click.option("-A", "--ignore-all-variable-attrs", default=False)
@click.option("--auto-cache/--no-auto-cache", default=False)
@click.option("-l", "--log-mode", default="standard")
@click.option("-v", "--verbose/--no-verbose", default=False)
@click.option("-t", "--template", default="auto")
"""

def parse_s3_config_file(fpath):
    return json.load(open(fpath))

@main.command()
@click.argument("file_uris", nargs=-1)
@click.option("-f", "--file-uris-file", default=None)
@click.option("-p", "--prefix", default=DEFAULTS["prefix"])
@click.option("-o", "--output-path", default=DEFAULTS["output_path"])
@click.option("-b", "--max-bytes", default=DEFAULTS["max_bytes"])
def create(file_uris, file_uris_file=None, prefix=DEFAULTS["prefix"], 
           output_path=DEFAULTS["output_path"], max_bytes=DEFAULTS["max_bytes"],
           s3_config_file=None):
#def create(is3_config=None, max_bytes=-1),file_uris, prefix, output_path="index.json", max_bytes=-1) 

#def create(file_path, mappings=None, rules=None, ignore_attrs=None, ignore_all_globals=False,
#          ignore_all_dimensions=False, ignore_all_variables=False, ignore_all_variable_attrs=False,
#          auto_cache=False, log_mode="standard", verbose=False, template="auto"):

    s3_config = parse_s3_config_file(s3_config_file) if s3_config_file else None
    if file_uris and file_uris_file:
        raise ValueError(f"Tool does not support setting BOTH 'file_uris' and 'file_uris_file' options")

    file_uris = open(file_uris_file).strip().split() if file_uris_file else file_uris

    indexer = kct.indexer.Indexer(s3_config=s3_config, max_bytes=max_bytes)
    indexer.create(file_uris, prefix, output_path=output_path, max_bytes=max_bytes)


@main.command()
@click.argument("log_files", nargs=-1, default=None)
@click.option("-d", "--log-directory", default=None)
@click.option("--show-files/--no-show-files", default=False)
@click.option("-x", "--exclude", default=None)
@click.option("-e", "--exclude-file", default=None)
@click.option("--verbose/--no-verbose", default=False)
def example2(log_files=None, log_directory=None, show_files=False,
            exclude=None, exclude_file=None,
            verbose=False):

    if exclude:
        exclude = string_to_list(exclude)  
    else:
        exclude = []
    
    if exclude_file:
        if not os.path.isfile(exclude_file):
            raise Exception(f"'--exclude-file' does not point to a valid file")

        with open(exclude_file) as exfile:
            exclude.extend([exclude_pattern for exclude_pattern in exfile if exclude_pattern.strip()])

    #return do_something(log_files, log_directory=log_directory, show_files=show_files,
   #                  exclude=exclude, verbose=verbose)


@main.command()
@click.argument("check_ids", nargs=-1, default=None) 
@click.option("--verbose/--no-verbose", default=False)
def example3(check_ids=None, verbose=False):
    pass
    #return do_something(check_ids, verbose=verbose)


if __name__ == "__main__":
    sys.exit(main())  # pragma: no cover

