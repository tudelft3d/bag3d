#!/usr/bin/python3
# -*- coding: utf-8 -*-

"""Handles the whole flow of updating AHN files and BAG, and generating the 3D BAG"""

import os
import sys
from shutil import rmtree

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
from bag3d import exporter
from bag3d import quality

from pprint import pformat

def app(cli_args, here, log_conf):
    """The command line application
    
    Parameters
    ----------
    cli_args : list of strings
        Command line arguments
    here : string
        Path to the source code directory
    """
    schema = os.path.join(here, 'bag3d_cfg_schema.yml')
    args_in = args.parse_console_args(cli_args[1:])

    log_conf['handlers']['console']['level'] = args_in['loglevel']
    logging.config.dictConfig(log_conf)
    logger = logging.getLogger(__name__)

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

            ahn.download(path_lasinfo=cfg['path_lasinfo'],
                         ahn3_dir=ahn3_dir, 
                         ahn2_dir=ahn2_dir, 
                         tile_index_file=cfg["elevation"]["file"],
                         ahn3_file_pat=ahn3_fp,
                         ahn2_file_pat=ahn2_fp)


        if args_in['update_ahn_raster']:
            logger.info("Updating AHN 0.5m raster files")
            ahn.download_raster(conn, cfg, 
                                cfg["quality"]["ahn2_rast_dir"], 
                                cfg["quality"]["ahn3_rast_dir"], 
                                doexec=args_in['no_exec'])
    
        if args_in['import_tile_idx']:
            logger.info("Importing BAG tile index")
            bag.import_index(cfg['polygons']["file"], cfg["database"]["dbname"], 
                             cfg['polygons']["schema"], str(cfg["database"]["host"]), 
                             str(cfg["database"]["port"]), cfg["database"]["user"], 
                             cfg["database"]["pw"],
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
                             cfg["database"]["pw"],
                             doexec=args_in['no_exec'])


        if args_in['add_borders']:
            logger.info("Configuring AHN2-3 border tiles")
            border.create_border_table(conn, cfg, 
                                       doexec=args_in['no_exec'])
            border.update_file_date(conn, cfg, ahn2_dir, ahn2_fp, 
                                    doexec=args_in['no_exec'])


        if args_in['run_3dfier']:
            logger.info("Configuring batch3dfier")
            clip_prefix = "_clip3dfy_"
            logger.debug("clip_prefix is %s", clip_prefix)
            cfg_out = batch3dfier.configure_tiles(conn, cfg, clip_prefix)
            cfg_rest, cfg_ahn2, cfg_ahn3 = border.process(conn, cfg_out, ahn3_dir, 
                                                          ahn2_dir, 
                                                          export=False)
            for c in [cfg_rest, cfg_ahn2, cfg_ahn3]:
                # clean up previous files
                if os.path.isdir(c["output"]["dir"]):
                    rmtree(c["output"]["dir"], ignore_errors=True, onerror=None)
                    logger.debug("Deleted %s", c["output"]["dir"])
                try:
                    os.makedirs(c["output"]["dir"], exist_ok=False)
                    logger.debug("Created %s", c["output"]["dir"])
                except Exception as e:
                    logger.error(e)
                    sys.exit(1)
                
                logger.info("Running batch3dfier")
                res = process.run(conn, c, doexec=args_in['no_exec'])
                
                restart = 0
                while restart < 3:
                    if res is None:
                        break
                    elif len(res) == 0:
                        break
                    elif len(res) > 0:
                        restart += 1
                        logger.info("Restarting 3dfier with tiles %s", res)
                        c["input_polygons"]["tile_list"] = res
                        res = process.run(conn, c, doexec=args_in['no_exec'])
                
                if not os.listdir(c["output"]["dir"]):
                    logger.warning("3dfier failed completely for %s, skipping import", 
                                   c["config"]["in"])
                else:
                    logger.info("Importing batch3dfier output into database")
                    importer.import_csv(conn, c)
            
            logger.info("Joining 3D tables")
            importer.unite_border_tiles(conn, cfg["output"]["schema"], 
                                        cfg_ahn2["output"]["bag3d_table"], 
                                        cfg_ahn3["output"]["bag3d_table"])
            importer.create_bag3d_table(conn, cfg["output"]["schema"],
                                        cfg["output"]["bag3d_table"])
            
            logger.info("Cleaning up")
            importer.drop_border_view(conn, cfg["output"]["schema"])
            for c in [cfg_rest, cfg_ahn2, cfg_ahn3]:
                importer.drop_border_table(conn, c)


        if args_in["grant_access"]:
            bag.grant_access(conn, args_in["grant_access"], 
                             cfg['tile_schema'], 
                             cfg['polygons']["schema"])


        if args_in['export']:
            logger.info("Exporting 3D BAG")
            exporter.csv(conn, cfg, cfg["output"]["dir"])
            exporter.gpkg(conn, cfg, cfg["output"]["dir"], args_in['no_exec'])
            exporter.postgis(conn, cfg, cfg["output"]["dir"], args_in['no_exec'])


        if args_in["quality"]:
            logger.info("Checking 3D BAG quality")
#             cfg_quality = quality.create_quality_views(conn, cfg)
            quality.create_quality_table(conn)
            quality.get_counts(conn, cfg)


        # Clean up
        for c in [cfg['config']['out_border_ahn2'], 
                  cfg['config']['out_border_ahn3'],
                  cfg['config']['out_rest']]:
            rmtree(os.path.dirname(c), ignore_errors=True)
#           rmtree(c["output"]["dir"], ignore_errors=True)

#             rast_idx = ahn.rast_file_idx(conn, cfg, 
#                                          cfg["quality"]["ahn2_rast_dir"], 
#                                          cfg["quality"]["ahn3_rast_dir"])
#             sample = quality.get_sample(conn, cfg_quality)
#             logger.info("Sample size %s", len(sample))
#             logger.debug(sample[0])
#             stats=['percentile_0.00', 'percentile_0.10', 'percentile_0.25',
#            'percentile_0.50', 'percentile_0.75', 'percentile_0.90',
#            'percentile_0.95', 'percentile_0.99']
#             reference = quality.compute_stats(sample, rast_idx, stats)
#             diffs,fields = quality.compute_diffs(reference, stats)
#             logger.info("Computed differences on %s buildings", len(diffs))
#             
#             out_dir = os.path.dirname(cfg["quality"]["results"])
#             os.makedirs(out_dir, exist_ok=True)
#             logger.info("Writing height comparison to %s",
#                                 cfg["quality"]["results"])
#             with open(cfg["quality"]["results"], 'w') as csvfile:
#                 writer = DictWriter(csvfile, fieldnames=fields)
#                 writer.writeheader()
#                 for row in diffs:
#                     writer.writerow(row)
#             
#             r = quality.compute_rmse(diffs, stats)
#             logger.info("RMSE across the whole sample %s",
#                                 pformat(r))
    except Exception as e:
        logger.exception(e)
    finally:
        conn.close()
        logging.shutdown()
