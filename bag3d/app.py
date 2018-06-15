#!/usr/bin/python3
# -*- coding: utf-8 -*-

"""Handles the whole flow of updating AHN files and BAG, and generating the 3D BAG"""

import os
import sys

import yaml
import logging, logging.config

from bag3d.config import args
from bag3d.config import db
from bag3d.config import footprints
from bag3d.config import border
from bag3d.config import batch3dfier
from bag3d.update import bag
from bag3d.update import ahn
from bag3d.batch3dfier import process
from bag3d import importer

from pprint import pformat

logger = logging.getLogger('app')

def app(cli_args, here):
    schema = os.path.join(here, 'bag3d_cfg_schema.yml')
    args_in = args.parse_console_args(cli_args[1:])
    
    try:
        cfg = args.parse_config(args_in, schema)
    except Exception as e:
        logger.exception("Couldn't parse configuration file")
        logger.exception(e)
        sys.exit(1)
    
    logger.debug(pformat(cfg))
    
    try:
        conn = db.db(
            dbname=cfg["database"]["dbname"],
            host=str(cfg["database"]["host"]),
            port=str(cfg["database"]["port"]),
            user=cfg["database"]["user"],
            password=cfg["database"]["pw"])
    except Exception as e:
        logger.exception(e)
        sys.exit(1)
    
    try:
        # well, let's assume the user provided the AHN3 dir first
        ahn3_fp = cfg["input_elevation"]["dataset_name"][0]
        ahn2_fp = cfg["input_elevation"]["dataset_name"][1]
        ahn3_dir = cfg["input_elevation"]["dataset_dir"][0]
        ahn2_dir = cfg["input_elevation"]["dataset_dir"][1]
        
        if args_in['update_bag']:
            logger.info("Updating BAG database")
            # At this point an empty database should exists, restore_BAG 
            # takes care of the rest
            bag.restore_BAG(cfg["database"], doexec=args_in['no_exec'])
    
        if args_in['update_ahn']:
            logger.info("Updating AHN files")

            ahn.download(ahn3_dir=ahn3_dir, 
                         ahn2_dir=ahn2_dir, 
                         tile_index_file=cfg["elevation"]["file"],
                         ahn3_file_pat=ahn3_fp,
                         ahn2_file_pat=ahn2_fp)
    
        if args_in['import_tile_idx']:
            logger.info("Importing BAG tile index")
            bag.import_index(cfg['polygons']["file"], cfg["database"]["dbname"], 
                             cfg['polygons']["schema"], str(cfg["database"]["host"]), 
                             str(cfg["database"]["port"]), cfg["database"]["user"], 
                             doexec=args_in['no_exec'])
            # Update BAG tiles to include the lower/left boundary
            footprints.update_tile_index(conn,
                                         table_index=[cfg['polygons']["schema"], 
                                                      cfg['polygons']["table"]],
                                         fields_index=[cfg['polygons']["fields"]["primary_key"], 
                                                       cfg['polygons']["fields"]["geometry"], 
                                                       cfg['polygons']["fields"]["unit_name"]]
                                         )
            logger.info("Partitioning the BAG")
            logger.debug("Creating centroids")
            footprints.create_centroids(conn,
                                        table_centroid=[cfg['footprints']["schema"], 
                                                        "pand_centroid"],
                                        table_footprint=[cfg['footprints']["schema"], 
                                                         cfg['footprints']["table"]],
                                        fields_footprint=[cfg['footprints']["fields"]["primary_key"], 
                                                          cfg['footprints']["fields"]["geometry"]]
                                        )
            logger.debug("Creating tiles")
            footprints.create_views(conn, schema_tiles=cfg['tile_schema'], 
                                     table_index=[cfg['polygons']["schema"], 
                                                  cfg['polygons']["table"]],
                                     fields_index=[cfg['polygons']["fields"]["primary_key"], 
                                                   cfg['polygons']["fields"]["geometry"], 
                                                   cfg['polygons']["fields"]["unit_name"]],
                                     table_centroid=[cfg['footprints']["schema"], "pand_centroid"],
                                     fields_centroid=[cfg['footprints']["fields"]["primary_key"], 
                                                      "geom"],
                                     table_footprint=[cfg['footprints']["schema"], 
                                                      cfg['footprints']["table"]],
                                     fields_footprint=[cfg['footprints']["fields"]["primary_key"], 
                                                       cfg['footprints']["fields"]["geometry"],
                                                       cfg['footprints']["fields"]["uniqueid"]
                                                       ],
                                     prefix_tiles=cfg['prefix_tile_footprint'])
            
            logger.info("Importing AHN tile index")
            bag.import_index(cfg['elevation']["file"], cfg["database"]["dbname"], 
                             cfg['elevation']["schema"], str(cfg["database"]["host"]), 
                             str(cfg["database"]["port"]), cfg["database"]["user"], 
                             doexec=args_in['no_exec'])
            
        if args_in['add_borders']:
            logger.info("Configuring AHN2-3 border tiles")
            border.create_border_table(conn, cfg, 
                                       doexec=args_in['no_exec'])
            border.update_file_date(conn, cfg, ahn2_dir, ahn2_fp, 
                                    doexec=args_in['no_exec'])
    
        if args_in['run_3dfier']:
            logger.info("Configuring batch3dfier")
            #TODO: need to add tile list preprocessing here
            cfg_rest, cfg_ahn2, cfg_ahn3 = border.process(conn, cfg, ahn3_dir, ahn2_dir, 
                                     export=False)
            for c in [cfg_rest, cfg_ahn2, cfg_ahn3]:
                logger.debug(pformat(cfg))
            
            clip_prefix = "_clip3dfy_"
            logger.debug("clip_prefix is %s", clip_prefix)
            
            for c in [cfg_rest, cfg_ahn2, cfg_ahn3]:
                cfg_out = batch3dfier.configure_tiles(conn, c, clip_prefix)
                logger.debug(cfg_out)
                if not os.path.exists(cfg_out["output"]["dir"]):
                    os.makedirs(cfg_out["output"]["dir"], exist_ok=True)
                logger.debug("Created %s", cfg_out["output"]["dir"])
                
                logger.info("Running batch3dfier")
                process.run(conn, cfg_out, doexec=args_in['no_exec'])
            
                logger.info("Importing batch3dfier output into database")
                importer.import_csv(conn, cfg_out)
            
            logger.info("Joining 3D tables")
            importer.unite_border_tiles(conn, cfg["output"]["schema"], 
                                        cfg_ahn2["output"]["bag3d_table"], 
                                        cfg_ahn3["output"]["bag3d_table"])
            importer.create_bag3d_table(conn, cfg["output"]["schema"])
        
        if args_in["grant_access"]:
            bag.grant_access(conn, args_in["grant_access"], 
                             cfg['tile_schema'], 
                             cfg['polygons']["schema"])
    
        if args_in['export']:
            logger.info("Exporting 3D BAG")
    except Exception as e:
        logger.exception(e)
    finally:
        conn.close()
