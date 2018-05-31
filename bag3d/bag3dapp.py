#!/usr/bin/python3
# -*- coding: utf-8 -*-

"""Handles the whole flow of updating AHN files and BAG, and generating the 3D BAG"""

import sys
import os.path
from sys import argv

import argparse
import yaml
import logging, logging.config

from bag3d.config import args

with open('logging.conf', 'r') as f:
    log_conf = yaml.safe_load(f)
logging.config.dictConfig(log_conf)
logger = logging.getLogger('bag3dapp')


def main():
    
    args_in = args.parse_console_args(sys.argv[1:])
    
    logger.debug("Parsing configuration file")
    
    cfg = args.parse_config(args_in)
    

    if args_in['create_db']:
        logger.info("Creating BAG database")

    if args_in['update_ahn']:
        logger.info("Updating AHN files")

    if args_in['import_tile_idx']:
        logger.info("Importing tile indexes")


    if args_in['update_bag']:
        logger.info("Updating the BAG database")


    if args_in['run_3dfier']:
        logger.info("Parsing batch3dfier configuration")

        logger.info("Configuring AHN2-3 border tiles")
        
        logger.info("Running batch3dfier")
        
        logger.info("Importing batch3dfier output into database")
        
        logger.info("Joining 3D tables")

    if args_in['export']:
        logger.info("Exporting 3D BAG")
    
    cfg["dbase"].close()

if __name__ == '__main__':
    main()
