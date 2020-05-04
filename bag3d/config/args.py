# -*- coding: utf-8 -*-

"""Parse arguments and configuration file"""

from sys import exit
import os.path
from shutil import rmtree
import argparse
import logging

import yaml
import pykwalify.core
import pykwalify.errors

logger = logging.getLogger(__name__)

def parse_console_args(args):
    """Parse command line arguments

    Parameters
    ----------
    args : list
        The list of command line arguments passed to bag3d.app.py

    Returns
    -------
    dict
        The stored argument values
    """
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
        "--update-ahn-raster",
        dest='update_ahn_raster',
        action="store_true",
        help="Download/update the AHN 0.5m raster files. All AHN3, and AHN2 where AHN3 is not available.")
    parser.add_argument(
        "--import-tile-idx",
        dest='import_tile_idx',
        action="store_true",
        help="Import the BAG and AHN tile indexes into the BAG database")
    parser.add_argument(
        "--add-borders",
        dest='add_borders',
        action="store_true",
        help="Create and configure the tiles on the AHN2-3 boundary")
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
    parser.add_argument(
        "--check-quality",
        action="store_true",
        dest='quality',
        help="Run various quality tests on the 3D BAG")
    parser.add_argument(
        "--no-exec",
        dest="no_exec",
        action="store_false",
        help="Control the execution of subprocesses. Used for debugging.")
    parser.add_argument(
        "--log",
        dest="loglevel",
        default="INFO",
        help="Set logging level.")
    parser.set_defaults(get_bag=False)
    parser.set_defaults(update_bag=False)
    parser.set_defaults(update_ahn=False)
    parser.set_defaults(update_ahn_raster=False)
    parser.set_defaults(import_tile_idx=False)
    parser.set_defaults(add_borders=False)
    parser.set_defaults(run_3dfier=False)
    parser.set_defaults(export=False)
    parser.set_defaults(quality=False)
    parser.set_defaults(no_exec=True)

    args = parser.parse_args(args)
    args_in = {}
    numeric_level = getattr(logging, args.loglevel.upper(), None)
    if not isinstance(numeric_level, int):
        raise ValueError('Invalid log level: %s' % args.loglevel)
    args_in['loglevel'] = args.loglevel.upper()
    args_in['cfg_file'] = os.path.abspath(args.path)
    if not os.path.exists(args_in['cfg_file']):
        raise FileNotFoundError('Configuration file %s not found' % args_in['cfg_file'])
    args_in['cfg_dir'] = os.path.dirname(args_in['cfg_file'])
    args_in['threads'] = args.threads
    args_in['get_bag'] = args.get_bag
    args_in['update_bag'] = args.update_bag
    args_in['update_ahn'] = args.update_ahn
    args_in['update_ahn_raster'] = args.update_ahn_raster
    args_in['import_tile_idx'] = args.import_tile_idx
    args_in['add_borders'] = args.add_borders
    args_in['run_3dfier'] = args.run_3dfier
    args_in['export'] = args.export
    args_in['quality'] = args.quality
    args_in['grant_access'] = args.grant_access
    args_in['no_exec'] = args.no_exec

    return args_in


def add_abspath(dirs):
    """Recursively append the absolute path to the paths in a nested list
    
    If not a list, returns the string with absolute path.
    
    Parameters
    ----------
    dirs : list of strings, or string
        List of directory paths
    
    Returns
    -------
    list
        List of absolute paths
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
    """Validates the configuration file against the schema
    
    The schema is located in bag3d/bag3d_cfg_schema.yml
    
    Parameters
    ----------
    config : str
        Path to the configuration file
    schema : str
        Path to the schema
    
    Raises
    ------
    FileNotFoundError
        If the schema is not found
    PyKwalifyException
        If the config file is not valid
    
    Returns
    -------
    None
    """
    if not os.path.exists(schema):
        logger.exception('Schema file %s not round', schema)
        raise FileNotFoundError
    try:
        c = pykwalify.core.Core(source_file=config, schema_files=[schema])
        c.validate(raise_exception=True)
    except pykwalify.errors.PyKwalifyException:
        logger.exception("Configuration file is not valid")
        raise


def parse_config(args_in, schema):
    """Process the configuration file
    
    Validates the config file and checks a few values
    
    Parameters
    ----------
    args_in : dict
        Output of config.parse_console_args()
    schema : str
        Path to the config file schema
    
    Returns
    -------
    dict
        The configuration parameters
    """
    cfg = {}

    # -- Get command line parameters, configure temporary files, validate config file
    try:
        validate_config(args_in['cfg_file'], schema)
        logger.info("Configuration file is valid")
        with open(args_in['cfg_file'], "r") as stream:
            cfg_stream = yaml.load(stream)
    except pykwalify.errors.PyKwalifyException:
        raise

    cfg['config'] = {}
    cfg['config']['in'] = args_in['cfg_file']
    rootdir = os.path.dirname(args_in['cfg_file'])
    rest_dir = os.path.join(rootdir, "cfg_rest")
    ahn2_dir = os.path.join(rootdir, "cfg_ahn2")
    ahn3_dir = os.path.join(rootdir, "cfg_ahn3")
    for d in [rest_dir, ahn2_dir, ahn3_dir]:
        if os.path.isdir(d):
            rmtree(d, ignore_errors=True, onerror=None)
        try:
            os.makedirs(d, exist_ok=False)
            logger.debug("Created %s", d)
        except Exception as e:
            logger.error(e)
    cfg['config']['out_rest'] = os.path.join(rest_dir, "bag3d_cfg_rest.yml")
    cfg['config']['out_border_ahn2'] = os.path.join(ahn2_dir, "bag3d_cfg_border_ahn2.yml")
    cfg['config']['out_border_ahn3'] = os.path.join(ahn3_dir, "bag3d_cfg_border_ahn3.yml")
    cfg['config']['threads'] = int(args_in['threads'])

    #-- Get config file parameters
    # database connection
    cfg['database'] = cfg_stream['database']

    # 2D polygons
    cfg['input_polygons'] = cfg_stream['input_polygons']
    try:
        # in case user gave " " or "" for 'extent'
        if len(cfg_stream['input_polygons']['extent']) <= 1:
            EXTENT_FILE = None
            logger.debug('extent string has length <= 1')
        cfg['input_polygons']['extent_file'] = os.path.abspath(
            cfg_stream['input_polygons']['extent'])
        cfg['input_polygons']['tile_list'] = None
    except (NameError, AttributeError, TypeError):
        tile_list = cfg_stream['input_polygons']['tile_list']
        assert isinstance(
            tile_list, list), "Please provide input for tile_list as a list: [...]"
        cfg['input_polygons']['tile_list'] = tile_list
        cfg['input_polygons']['extent_file'] = None
    # 'user_schema' is used for the '_clip3dfy_' and '_union' views, thus
    # only use 'user_schema' if 'extent' is provided
    USER_SCHEMA = cfg_stream['input_polygons']['user_schema']
    if (USER_SCHEMA is None) or (EXTENT_FILE is None):
        logger.debug("user_schema or extent is None")
        cfg['input_polygons']['user_schema'] = cfg['input_polygons']['tile_schema']

    # AHN point cloud
    cfg['input_elevation'] = cfg_stream['input_elevation']
    cfg['input_elevation']['dataset_dir'] = add_abspath(
        cfg_stream['input_elevation']['dataset_dir'])

    # quality checks
    if cfg_stream['quality']['ahn2_rast_dir']:
        os.makedirs(cfg_stream['quality']['ahn2_rast_dir'], exist_ok=True)
    if cfg_stream['quality']['ahn3_rast_dir']:
        os.makedirs(cfg_stream['quality']['ahn3_rast_dir'], exist_ok=True)
    cfg['quality'] = cfg_stream['quality']

    # partitioning of the 2D polygons
    cfg['tile_index'] = cfg_stream['tile_index']

    # output control
    cfg['output'] = cfg_stream['output']
    cfg['output']['staging']['dir'] = os.path.abspath(cfg_stream['output']['staging']['dir'])
    os.makedirs(cfg['output']['staging']['dir'], exist_ok=True)
    cfg['output']['production']['dir'] = os.path.abspath(cfg_stream['output']['production']['dir'])
    os.makedirs(cfg['output']['production']['dir'], exist_ok=True)

    # executables
    cfg['path_3dfier'] = cfg_stream['path_3dfier']
    cfg['path_lasinfo'] = cfg_stream['path_lasinfo']

    return cfg