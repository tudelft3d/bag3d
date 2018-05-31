# -*- coding: utf-8 -*-

"""Parse configuration"""

import sys
import os.path
import argparse
import warnings
import logging

import yaml
import pykwalify.core
import pykwalify.errors

from bag3d.config import db

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
    parser.add_argument(
        "--create_db",
        action="store_true",
        help="Create a new database for the BAG")
    parser.add_argument(
        "--update_bag",
        action="store_true",
        help="Update the BAG in the database")
    parser.add_argument(
        "--update_ahn",
        action="store_true",
        help="Download/update the AHN files")
    parser.add_argument(
        "--import_tile_idx",
        action="store_true",
        help="Import the BAG and AHN tile indexes into the BAG database")
    parser.add_argument(
        "--run_3dfier",
        action="store_true",
        help="Run batch3dfier")
    parser.add_argument(
        "--export",
        action="store_true",
        help="Export the 3D BAG into files")
    parser.set_defaults(create_db=False)
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
        sys.exit(1)
    args_in['cfg_dir'] = os.path.dirname(args_in['cfg_file'])
    args_in['threads'] = args.threads
    args_in['create_db'] = args.create_db
    args_in['update_bag'] = args.update_bag
    args_in['update_ahn'] = args.update_ahn
    args_in['import_tile_idx'] = args.import_tile_idx
    args_in['run_3dfier'] = args.run_3dfier
    args_in['export'] = args.export
    
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
    c = pykwalify.core.Core(source_file=config, schema_files=[schema])
    try:
        c.validate(raise_exception=True)
        return True
    except pykwalify.errors.PyKwalifyException as e:
        logger.exception("Configuration file is not valid")
        return False


def parse_config(args_in):
    """Process the configuration file"""
    cfg = {}
    
    schema = os.path.abspath('bag3d_config_schema.yml')
    if not os.path.exists(schema):
        logger.exception('Schema file %s not round', schema)
        sys.exit(1)
    else:
        v = validate_config(args_in['cfg_file'], schema)
        if v == True:
            with open(args_in['cfg_file'], "r") as stream:
                cfg_stream = yaml.load(stream)
        else:
            sys.exit(1)

    cfg['pc_dataset_name'] = cfg_stream["input_elevation"]["dataset_name"]
    cfg['pc_dir'] = add_abspath(
        cfg_stream["input_elevation"]["dataset_dir"])
#     cfg['pc_tile_case'] = cfg_stream["input_elevation"]["tile_case"]
    cfg['polygons'] = cfg_stream['tile_index']['polygons']
    cfg['elevation'] = cfg_stream['tile_index']['elevation']

    OUTPUT_FORMAT = cfg_stream["output"]["format"]
    if all(f not in OUTPUT_FORMAT.lower() for f in ["csv", "obj"]):
        warnings.warn(
            "\n No file format is appended to output. Currently only .obj or .csv is handled.\n")
    cfg['output_format'] = OUTPUT_FORMAT
    cfg['output_dir'] = os.path.abspath(cfg_stream["output"]["dir"])
    if not os.path.exists(cfg['output_dir']):
        os.makedirs(cfg['output_dir'], exist_ok=True)
    if 'CSV-BUILDINGS-MULTIPLE' == cfg['output_format']:
        cfg['out_schema'] = cfg_stream["output"]["schema"]
        cfg['out_table'] = cfg_stream["output"]["table"]
        cfg['bag3d_table'] = cfg_stream["output"]["bag3d_table"]
    else:
        # OBJ is not imported into postgres
        cfg['out_schema'] = None
        cfg['out_table'] = None
        pass

    cfg['path_3dfier'] = cfg_stream["path_3dfier"]

    try:
        # in case user gave " " or "" for 'extent'
        if len(cfg_stream["input_polygons"]["extent"]) <= 1:
            EXTENT_FILE = None
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
        cfg['user_schema'] = cfg['tile_schema']

    # Connect to database ----------------------------------------------------
    cfg['dbase'] = db.db(
        dbname=cfg_stream["database"]["dbname"],
        host=str(cfg_stream["database"]["host"]),
        port=cfg_stream["database"]["port"],
        user=cfg_stream["database"]["user"],
        password=cfg_stream["database"]["pw"])

    cfg['uniqueid'] = cfg_stream["input_polygons"]['uniqueid']

    cfg['prefix_tile_footprint'] = cfg_stream["input_polygons"]["tile_prefix"]

    return(cfg)
