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

from bag3d.config import batch3dfier
from bag3d.config import db


logger = logging.getLogger('batch3dfier.process')


def run(conn, config, doexec=True):
    # Prefix for naming the clipped/united views. This value shouldn't be a
    # substring in the pointcloud file names.
    
    # used in call3dfier()
    tile_out = None
    ewkb = None
    union_view = None
    tiles_clipped = None

    #FIXME: implement that connection pool
    conn = config['dbase']
    tiles = config['tiles']
    pc_name_map = batch3dfier.pc_name_dict(config['pc_dir'], 
                                           config['pc_dataset_name'])
    pc_file_idx = batch3dfier.pc_file_index(pc_name_map)


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
                    db=conn,
                    tile=tile,
                    schema_tiles=config['user_schema'],
                    table_index_pc=config['elevation'],
                    fields_index_pc=config['elevation']['fields'],
                    table_index_footprint=config['polygons'],
                    fields_index_footprint=config['polygons']['fields'],
                    uniqueid=config["footprints"]["fields"]['uniqueid'],
                    extent_ewkb=ewkb,
                    clip_prefix=config["clip_prefix"],
                    prefix_tile_footprint=config['prefix_tile_footprint'],
                    yml_dir=args_in['config_dir'],
                    tile_out=tile_out,
                    output_format=config['output_format'],
                    output_dir=config['output_dir'],
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
            conn,
            config['user_schema'],
            views_to_drop=tiles_clipped)


    # Delete temporary config files
    yml_config = [
        os.path.join(
            args_in['cfg_dir'],
            t +
            "_config.yml") for t in threadList]
    command = "rm"
    for c in yml_config:
        command = command + " " + c
    call(command, shell=True)
    
    conn.close()

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
