#!/usr/bin/python3
# -*- coding: utf-8 -*-

"""The batch3dfier application."""

import os
import queue
import threading
import time
from shutil import rmtree
import logging

from bag3d.config import batch3dfier

logger = logging.getLogger('batch3dfier.process')

def run(conn, config, doexec=True):
    tiles = config["input_polygons"]["tile_list"]
    cfg_dir = os.path.dirname(config["config"]["in"])
    pc_name_map = batch3dfier.pc_name_dict(config["input_elevation"]["dataset_dir"], 
                                           config["input_elevation"]["dataset_name"])
    pc_file_idx = batch3dfier.pc_file_index(pc_name_map)

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
                logger.debug("%s processing %s" % (threadName, tile))
                t = batch3dfier.call_3dfier(
                    db=conn,
                    tile=tile,
                    schema_tiles=config["input_polygons"]['user_schema'],
                    table_index_pc=config["tile_index"]['elevation'],
                    fields_index_pc=config["tile_index"]['elevation']['fields'],
                    table_index_footprint=config["tile_index"]['polygons'],
                    fields_index_footprint=config["tile_index"]['polygons']['fields'],
                    uniqueid=config["input_polygons"]["footprints"]["fields"]['uniqueid'],
                    extent_ewkb=config["extent_ewkb"],
                    clip_prefix=config["clip_prefix"],
                    prefix_tile_footprint=config["input_polygons"]['tile_prefix'],
                    yml_dir=cfg_dir,
                    tile_out=config["tile_out"],
                    output_format='CSV-BUILDINGS-MULTIPLE',
                    output_dir=config['output']['dir'],
                    path_3dfier=config['path_3dfier'],
                    thread=threadName,
                    pc_file_index=pc_file_idx,
                    doexec=doexec)
                if t['tile_skipped'] is not None:
                    tiles_skipped.append(t['tile_skipped'])
                else:
                    out_paths.append(t['out_path'])
            else:
                queueLock.release()

    # Prep
    threadList = ["Thread-" + str(t + 1) for t in range(config['threads'])]
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
    for tile in tiles:
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
    try:
        to_drop = [tile for tile in tiles if 
                   config["clip_prefix"] in tile or 
                   config["tile_out"] in tile]
        if to_drop:
            batch3dfier.drop_2Dtiles(
                conn,
                config["input_polygons"]['user_schema'],
                views_to_drop=to_drop)
    except TypeError:
        logger.debug("No views to drop")
    rmtree(cfg_dir, ignore_errors=True)
    
    # Reporting
    tiles = set(tiles)
    tiles_skipped = set(tiles_skipped)
    logger.info("Total number of tiles processed: %s",
                 str(len(tiles.difference(tiles_skipped))))
    logger.info("Tiles skipped: %s", tiles_skipped)
