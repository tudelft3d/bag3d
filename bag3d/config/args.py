# -*- coding: utf-8 -*-

"""Parse configuration"""

from sys import exit
import os.path
import argparse
import logging

import yaml
import pykwalify.core
import pykwalify.errors

logger = logging.getLogger('config.args')

def parse_console_args(args):
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description="Generate a 3D BAG")
    parser.add_argument(
        "path",
        help="The YAML configuration file")
    parser.add_argument(
        "-t", "--threads",
        help="The number of threads to run.",
        default=3,
        type=int)
#     parser.add_argument(
#         "--get-bag",
#         dest='get_bag',
#         action="store_true",
#         help="Download and restore the BAG extract into the database")
    parser.add_argument(
        "--update-bag",
        dest='update_bag',
        action="store_true",
        help="Update the BAG in the database. If it does not exists, download and restore the BAG extract into the database")
    parser.add_argument(
        "--update-ahn",
        dest='update_ahn',
        action="store_true",
        help="Download/update the AHN files")
    parser.add_argument(
        "--import-tile-idx",
        dest='import_tile_idx',
        action="store_true",
        help="Import the BAG and AHN tile indexes into the BAG database")
    parser.add_argument(
        "--run-3dfier",
        dest='run_3dfier',
        action="store_true",
        help="Run batch3dfier")
    parser.add_argument(
        "--grant-access",
        dest='grant_access',
        type=str,
        help="Grant the necessary privileges to a user to operate on a 3DBAG database")
    parser.add_argument(
        "--export",
        action="store_true",
        help="Export the 3D BAG into files")
    parser.set_defaults(get_bag=False)
    parser.set_defaults(update_bag=False)
    parser.set_defaults(update_ahn=False)
    parser.set_defaults(import_tile_idx=False)
    parser.set_defaults(run_3dfier=False)
    parser.set_defaults(export=False)

    args = parser.parse_args(args)
    args_in = {}
    args_in['cfg_file'] = os.path.abspath(args.path)
    if not os.path.exists(args_in['cfg_file']):
        logger.exception('Configuration file %s not round', args_in['cfg_file'])
        exit(1)
    args_in['cfg_dir'] = os.path.dirname(args_in['cfg_file'])
    args_in['threads'] = args.threads
    args_in['get_bag'] = args.get_bag
    args_in['update_bag'] = args.update_bag
    args_in['update_ahn'] = args.update_ahn
    args_in['import_tile_idx'] = args.import_tile_idx
    args_in['run_3dfier'] = args.run_3dfier
    args_in['export'] = args.export
    args_in['grant_access'] = args.grant_access
    
    return args_in


def add_abspath(dirs):
    """Recursively append the absolute path to the paths in a nested list
    
    If not a list, returns the string with abolute path.
    """
    if isinstance(dirs, list):
        for i, elem in enumerate(dirs):
            if isinstance(elem, str):
                dirs[i] = os.path.abspath(elem)
            else:
                dirs[i] = add_abspath(elem)
        return dirs
    else:
        return os.path.abspath(dirs)


def validate_config(config, schema):
    """Validates the configuration file against the schema"""
    if not os.path.exists(schema):
        logger.exception('Schema file %s not round', schema)
        raise FileNotFoundError
    try:
        c = pykwalify.core.Core(source_file=config, schema_files=[schema])
        c.validate(raise_exception=True)
    except pykwalify.errors.PyKwalifyException:
        logger.exception("Configuration file is not valid")
        raise


def parse_config(args_in):
    """Process the configuration file"""
    cfg = {}
    
    schema = os.path.abspath('bag3d_config_schema.yml')
    try:
        validate_config(args_in['cfg_file'], schema)
        logger.info("Configuration file is valid")
        with open(args_in['cfg_file'], "r") as stream:
            cfg_stream = yaml.load(stream)
    except pykwalify.errors.PyKwalifyException:
        raise

    cfg['pc_dataset_name'] = cfg_stream["input_elevation"]["dataset_name"]
    cfg['pc_dir'] = add_abspath(
        cfg_stream["input_elevation"]["dataset_dir"])
    cfg['polygons'] = cfg_stream['tile_index']['polygons']
    cfg['elevation'] = cfg_stream['tile_index']['elevation']

    cfg['output_dir'] = os.path.abspath(cfg_stream["output"]["dir"])
    if not os.path.exists(cfg['output_dir']):
        os.makedirs(cfg['output_dir'], exist_ok=True)
    
    cfg['out_schema'] = cfg_stream["output"]["schema"]
    cfg['out_table'] = cfg_stream["output"]["table"]
    cfg['bag3d_table'] = cfg_stream["output"]["bag3d_table"]

    cfg['path_3dfier'] = cfg_stream["path_3dfier"]

    try:
        # in case user gave " " or "" for 'extent'
        if len(cfg_stream["input_polygons"]["extent"]) <= 1:
            EXTENT_FILE = None
            logger.debug("extent string has length <= 1")
        cfg['extent_file'] = os.path.abspath(
            cfg_stream["input_polygons"]["extent"])
        cfg['tiles'] = None
    except (NameError, AttributeError, TypeError):
        tile_list = cfg_stream["input_polygons"]["tile_list"]
        assert isinstance(
            tile_list, list), "Please provide input for tile_list as a list: [...]"
        cfg['tiles'] = tile_list
        cfg['extent_file'] = None

    # 'user_schema' is used for the '_clip3dfy_' and '_union' views, thus
    # only use 'user_schema' if 'extent' is provided
    cfg['tile_schema'] = cfg_stream["input_polygons"]["tile_schema"]
    USER_SCHEMA = cfg_stream["input_polygons"]["user_schema"]
    if (USER_SCHEMA is None) or (EXTENT_FILE is None):
        logger.debug("user_schema or extent is None")
        cfg['user_schema'] = cfg['tile_schema']
    
    cfg['database'] = cfg_stream['database']

    cfg["footprints"] = cfg_stream["input_polygons"]["footprints"]

    cfg['prefix_tile_footprint'] = cfg_stream["input_polygons"]["tile_prefix"]

    return(cfg)