#!/usr/bin/python3
# -*- coding: utf-8 -*-

"""Handles the whole flow of updating AHN files and BAG, and generating the 3D BAG"""

import os.path
from sys import argv

import argparse
import logging
import logging.config

logfile = os.path.join(os.getcwd(), 'bag3d.log')
logging.basicConfig(filename=logfile,
                    filemode='a',
                    level=logging.DEBUG,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


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
    # FIXME: handle file not found
    args_in['cfg_file'] = os.path.abspath(args.path)
    args_in['cfg_dir'] = os.path.dirname(args_in['cfg_file'])
    args_in['threads'] = args.threads
    args_in['create_db'] = args.create_db
    args_in['update_bag'] = args.update_bag
    args_in['update_ahn'] = args.update_ahn
    args_in['import_tile_idx'] = args.import_tile_idx
    args_in['run_3dfier'] = args.run_3dfier
    args_in['export'] = args.export
    
    return args_in


def main():
    
    args_in = parse_console_args(sys.argv[1:])
    
#    put all the swtiches in a YAML config file
    logger.debug("Parsing configuration file")

    if args_in['create_db']:
        logger.debug("Creating BAG database")

    if args_in['update_ahn']:
        logger.debug("Updating AHN files")

#        update ahn in the BAG DB

    if args_in['import_tile_idx']:
        logger.debug("Importing tile indexes")


    if args_in['update_bag']:
        logger.debug("Updating the BAG database")

#    import the batch3dfier config
    logger.debug("Parsing batch3dfier configuration")

#    prepare the 3 configs for the border and rest tiles
    logger.info("Configuring AHN2-3 border tiles")

    if args_in['run_3dfier']:
        logger.info("Running batch3dfier")
        
        logger.info("Importing batch3dfier output into database")
        
        logger.info("Joining 3D tables")

    if args_in['export']:
        logger.info("Exporting 3D BAG")

if __name__ == '__main__':
    main()
