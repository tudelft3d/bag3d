#!/usr/bin/python3
# -*- coding: utf-8 -*-

"""The batch3dfier application."""


import os
import sys
import queue
import threading
import time
import warnings
import argparse
from subprocess import call
import logging

import yaml
from psycopg2 import sql
from pprint import pformat

from batch3dfier import config
from batch3dfier import db


logfile = os.path.join(os.getcwd(), 'batch3dfier.log')
logging.basicConfig(filename=logfile,
                    filemode='a',
                    level=logging.DEBUG,
                    format='%(asctime)s - %(levelname)s - %(message)s')


def parse_console_args():
    # Parse command-line arguments -------------------------------------------
    parser = argparse.ArgumentParser(description="Batch 3dfy 2D datasets.")
    parser.add_argument(
        "path",
        help="The YAML config file for batch3dfier. See batch3dfier_config.yml for an example.")
    parser.add_argument(
        "-t", "--threads",
        help="The number of threads to run.",
        default=3,
        type=int)

    args = parser.parse_args()
    args_in = {}
    # FIXME: handle file not found
    args_in['cfg_file'] = os.path.abspath(args.path)
    args_in['cfg_dir'] = os.path.dirname(args_in['cfg_file'])
    args_in['threads'] = args.threads

    return(args_in)


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


def parse_config_yaml(args_in):
    """Process the config YAML to internal format"""
    cfg = {}

    try:
        stream = open(args_in['cfg_file'], "r")
        cfg_stream = yaml.load(stream)
    except FileNotFoundError as e:
        logging.exception("Config file not found at %s", args_in['cfg_file'])
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


def main():
    # Prefix for naming the clipped/united views. This value shouldn't be a
    # substring in the pointcloud file names.
    CLIP_PREFIX = "_clip3dfy_"
    # used in call3dfier()
    tile_out = None
    ewkb = None
    union_view = None
    tiles_clipped = None

    args_in = parse_console_args()
    cfg = parse_config_yaml(args_in)
    dbase = cfg['dbase']
    tiles = cfg['tiles']
    pc_name_map = config.pc_name_dict(cfg['pc_dir'], cfg['pc_dataset_name'])
    pc_file_idx = config.pc_file_index(pc_name_map)

    # =========================================================================
    # Get tile list if 'extent' provided
    # =========================================================================
    # TODO: assert that CREATE/DROP allowed on TILE_SCHEMA and/or USER_SCHEMA
    if cfg['extent_file']:
        poly, ewkb = config.extent_to_ewkb(dbase, cfg['polygons'],
                                           cfg['extent_file'])

        tiles = config.get_2Dtiles(dbase, cfg['polygons'],
                                   cfg['polygons']['fields'], ewkb)

        # Get view names for tiles
        tile_views = config.get_2Dtile_views(dbase, cfg['tile_schema'], tiles)

        view_fields = config.get_view_fields(
            dbase, cfg['tile_schema'], tile_views)

        # clip 2D tiles to extent
        tiles_clipped = config.clip_2Dtiles(dbase, cfg['user_schema'],
                                            cfg['tile_schema'],
                                            tile_views, poly,
                                            CLIP_PREFIX,
                                            view_fields)

        # if the area of the extent is less than that of a tile, union the tiles is the
        # extent spans over many
        tile_area = config.get_2Dtile_area(dbase, cfg['polygons'])
        if len(tiles_clipped) > 1 and poly.area < tile_area:
            union_view = config.union_2Dtiles(dbase, cfg['user_schema'],
                                              tiles_clipped, CLIP_PREFIX,
                                              view_fields)
            tile_out = "output_batch3dfier"
        else:
            union_view = []

    elif tiles:
        # ======================================================================
        # Get tile list if 'tile_list' = 'all'
        # ======================================================================
        if 'all' in tiles:
            schema_q = sql.Identifier(cfg['polygons']['schema'])
            table_q = sql.Identifier(cfg['polygons']['table'])
            unit_q = sql.Identifier(cfg['polygons']['fields']['unit_name'])
            query = sql.SQL("""
                            SELECT {unit}
                            FROM {schema}.{table};
                            """).format(schema=schema_q, table=table_q,
                                        unit=unit_q)
            resultset = dbase.getQuery(query)
            tiles = [tile[0] for tile in resultset]
            tile_views = config.get_2Dtile_views(dbase, cfg['tile_schema'],
                                                 tiles)
        else:
            tile_views = config.get_2Dtile_views(dbase, cfg['tile_schema'],
                                                 tiles)
        
        if not tile_views or len(tile_views) == 0:
            print("There was an error, see the logfile")
            dbase.close()
            sys.exit(1)
        else:
            pass

    else:
        TypeError("Please provide either 'extent' or 'tile_list' in config.")
    # =========================================================================
    # Process multiple threads
    # reference: http://www.tutorialspoint.com/python3/python_multithreading.htm
    # =========================================================================
    logging.debug("tile_views: %s", tile_views)
    logging.debug("pc_file_idx: %s", pformat(pc_file_idx))


    exitFlag = 0
    tiles_skipped = []
    out_paths = []

    class myThread (threading.Thread):

        def __init__(self, threadID, name, q):
            threading.Thread.__init__(self)
            self.threadID = threadID
            self.name = name
            self.q = q

        def run(self):
            process_data(self.name, self.q)

    def process_data(threadName, q):
        while not exitFlag:
            queueLock.acquire()
            if not workQueue.empty():
                tile = q.get()
                queueLock.release()
                logging.debug("%s processing %s" % (threadName, tile))
                t = config.call_3dfier(
                    db=dbase,
                    tile=tile,
                    schema_tiles=cfg['user_schema'],
                    table_index_pc=cfg['elevation'],
                    fields_index_pc=cfg['elevation']['fields'],
                    table_index_footprint=cfg['polygons'],
                    fields_index_footprint=cfg['polygons']['fields'],
                    uniqueid=cfg['uniqueid'],
                    extent_ewkb=ewkb,
                    clip_prefix=CLIP_PREFIX,
                    prefix_tile_footprint=cfg['prefix_tile_footprint'],
                    yml_dir=args_in['cfg_dir'],
                    tile_out=tile_out,
                    output_format=cfg['output_format'],
                    output_dir=cfg['output_dir'],
                    path_3dfier=cfg['path_3dfier'],
                    thread=threadName,
                    pc_file_index=pc_file_idx)
                if t['tile_skipped'] is not None:
                    tiles_skipped.append(t['tile_skipped'])
                else:
                    out_paths.append(t['out_path'])
            else:
                queueLock.release()
                time.sleep(1)

    # Prep
    threadList = ["Thread-" + str(t + 1) for t in range(args_in['threads'])]
    queueLock = threading.Lock()
    workQueue = queue.Queue(0)
    threads = []
    threadID = 1

    # Create new threads
    for tName in threadList:
        thread = myThread(threadID, tName, workQueue)
        thread.start()
        threads.append(thread)
        threadID += 1

    # Fill the queue
    queueLock.acquire()
    if union_view:
        workQueue.put(union_view)
    elif tiles_clipped:
        for tile in tiles_clipped:
            workQueue.put(tile)
    else:
        for tile in tile_views:
            workQueue.put(tile)

    queueLock.release()

    # Wait for queue to empty
    while not workQueue.empty():
        pass

    # Notify threads it's time to exit
    exitFlag = 1

    # Wait for all threads to complete
    for t in threads:
        t.join()

    # Drop temporary views that reference the clipped extent
    if union_view:
        tiles_clipped.append(union_view)
    if tiles_clipped:
        config.drop_2Dtiles(
            dbase,
            cfg['user_schema'],
            views_to_drop=tiles_clipped)


    # Delete temporary config files
    yml_cfg = [
        os.path.join(
            args_in['cfg_dir'],
            t +
            "_config.yml") for t in threadList]
    command = "rm"
    for c in yml_cfg:
        command = command + " " + c
    call(command, shell=True)
    
    dbase.close()

    # =========================================================================
    # Reporting
    # =========================================================================
    tiles = set(tiles)
    tiles_skipped = set(tiles_skipped)
    logging.info("Total number of tiles processed: %s",
                 str(len(tiles.difference(tiles_skipped))))
    logging.info("Tiles skipped: %s", tiles_skipped)


if __name__ == '__main__':
    main()
