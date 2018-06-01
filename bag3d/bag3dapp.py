#!/usr/bin/python3
# -*- coding: utf-8 -*-

"""Handles the whole flow of updating AHN files and BAG, and generating the 3D BAG"""

from sys import argv, exit

import yaml
import logging, logging.config

from bag3d.config import args
from bag3d.config import db
from bag3d.update import bag

from pprint import pformat

with open('logging.conf', 'r') as f:
    log_conf = yaml.safe_load(f)
logging.config.dictConfig(log_conf)
logger = logging.getLogger('bag3dapp')


def main():
    
    logger.debug("Parsing arguments and configuration file")
    
    args_in = args.parse_console_args(argv[1:])
    
    try:
        cfg = args.parse_config(args_in)
    except:
        exit(1)
    
    logger.debug(pformat(cfg))
    
    try:
        conn = db.db(
            dbname=cfg["database"]["dbname"],
            host=str(cfg["database"]["host"]),
            port=cfg["database"]["port"],
            user=cfg["database"]["user"],
            password=cfg["database"]["pw"])
    except:
        exit(1)
    
    if args_in['get_bag']:
        logger.info("Restoring BAG database")
        # At this point an empty database should exists, restore_BAG 
        # takes care of the rest
        bag.restore_BAG(cfg["database"], doexec=False)

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
    
    conn.close()

if __name__ == '__main__':
    main()
