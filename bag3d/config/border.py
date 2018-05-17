#!/usr/bin/python3
# -*- coding: utf-8 -*-

"""Configure the batch3dfier processes for the tiles on AHN2 and AHN3 border"""


from os import path
from sys import exit
import warnings
import copy
import yaml
import psycopg2, psycopg2.sql
import logging

import pprint


logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s - %(levelname)s - %(message)s')

config = {
    'db': {
        'dbname': "bag_test",
        'host': "localhost",
        'port': "55555",
        'user': "bag_admin"
        },
    'tile_index': {
        'schema': "tile_index",
        'table': {
            'name': "ahn_index",
            'version': "ahn_version",
            'geom': "geom",
            'tile': "bladnr"
            },
        'border_table': 'border_tiles'
        },
    'ahn2': {
        'dir': "/data/pointcloud/AHN2/merged"
        },
    'ahn3': {
        'dir': "/data/pointcloud/AHN3/as_downloaded"
        },
    'config': {
        'in': "/home/bdukai/Data/3DBAG/batch3dfy_bag_test_area.yml",
        'out_rest': "/home/bdukai/Data/3DBAG/conf_test_rest.yml",
        'out_border_ahn2': "/home/bdukai/Data/3DBAG/conf_test_border_ahn2.yml",
        'out_border_ahn3': "/home/bdukai/Data/3DBAG/conf_test_border_ahn3.yml"
        }
    }


def parse_yml(file):
    """Parse a YAML config file"""
    try:
        stream = open(file, "r")
        cfg_stream = yaml.load(stream)
    except FileNotFoundError as e:
        logging.exception("Config file not found at %s", file)
        exit(1)
    return cfg_stream


def update_out_relations(cfg, ahn_version, ahn_dir, border_table):
    sfx = "_border_ahn" + str(ahn_version)
    try:
        name_idx = cfg["input_elevation"]["dataset_dir"].index(ahn_dir)
        n = cfg["input_elevation"]["dataset_name"][name_idx]
        cfg["input_elevation"]["dataset_name"] = n
    except ValueError as e:
        logging.error("Cannot find %s in input_elevation:dataset_dir \
        of batch3dfier config", ahn_dir)
        exit(1)
    # configure to use AHN2 only
    # schema is not expected to change for border_table
    cfg["tile_index"]["elevation"]["table"] = border_table
    cfg["input_elevation"]["dataset_dir"] = ahn_dir
    d = cfg["output"]["dir"]
    dname = path.join(path.dirname(d), path.basename(d) + sfx)
    cfg["output"]["dir"] = dname
    # FIXME: where to put and how to handle this bag3d_table?
    if cfg["output"]["table"]:
        t = cfg["output"]["table"]
        cfg["output"]["table"] = t + sfx
        tb = cfg["output"]["bag3d_table"]
        cfg["output"]["bag3d_table"] = tb + sfx
    else:
        cfg["output"]["schema"] = "public"
        cfg["output"]["table"] = "heights" + sfx
        cfg["output"]["bag3d_table"] = "bag3d" + sfx
    
    return cfg


def update_yml(yml, tile_list, ahn_version=None, ahn_dir=None, border_table=None):
    """Update the tile_list in the YAML config and write file
    
    Assumes that input_elevation:dataset_dir has at least 2 entries, one of
    them is the AHN2 directory.
    """
    c = copy.deepcopy(yml)
    c["input_polygons"]["tile_list"] = tile_list
    
    if ahn_version:
        c = update_out_relations(c, ahn_version, ahn_dir, border_table)
    else:
        sfx = "_rest"
        d = c["output"]["dir"]
        dname = path.join(path.dirname(d), path.basename(d) + sfx)
        c["output"]["dir"] = dname
        if c["output"]["table"]:
            t = c["output"]["table"]
            c["output"]["table"] = t + sfx
            tb = c["output"]["bag3d_table"]
            c["output"]["bag3d_table"] = tb + sfx
        else:
            c["output"]["schema"] = "public"
            c["output"]["table"] = "heights" + sfx
            c["output"]["bag3d_table"] = "bag3d" + sfx
    
    return c
    
#     if ahn_version == 2:
#         try:
#             name_idx = c["input_elevation"]["dataset_dir"].index(ahn_dir)
#             n = c["input_elevation"]["dataset_name"][name_idx]
#             c["input_elevation"]["dataset_name"] = n
#         except ValueError as e:
#             logging.error("Cannot find %s in input_elevation:dataset_dir \
#             of batch3dfier config", ahn_dir)
#             exit(1)
#         # configure to use AHN2 only
#         # schema is not expected to change for border_table
#         c["tile_index"]["elevation"]["table"] = border_table
#         c["input_elevation"]["dataset_dir"] = ahn_dir
#         d = c["output"]["dir"]
#         dname = path.join(path.dirname(d), path.basename(d) + "_border_ahn2")
#         c["output"]["dir"] = dname
#         # FIXME: where to put and how to handle this bag3d_table?
#         if c["output"]["table"]:
#             sfx = "_border_ahn2"
#             t = c["output"]["table"]
#             c["output"]["table"] = t + sfx
#             tb = c["output"]["bag3d_table"]
#             c["output"]["bag3d_table"] = tb + sfx
#         else:
#             c["output"]["schema"] = "public"
#             c["output"]["table"] = "heights_border_ahn2"
#             c["output"]["bag3d_table"] = "bag3d_border_ahn2"
#     elif ahn_version == 3:
#         try:
#             name_idx = c["input_elevation"]["dataset_dir"].index(ahn_dir)
#             n = c["input_elevation"]["dataset_name"][name_idx]
#             c["input_elevation"]["dataset_name"] = n
#         except ValueError as e:
#             logging.error("Cannot find %s in input_elevation:dataset_dir \
#             of batch3dfier config", ahn_dir)
#             exit(1)
#         c["tile_index"]["elevation"]["table"] = border_table
#         c["input_elevation"]["dataset_dir"] = ahn_dir
#         d = c["output"]["dir"]
#         dname = path.join(path.dirname(d), path.basename(d) + "_border_ahn3")
#         c["output"]["dir"] = dname
#         # FIXME: where to put and how to handle this bag3d_table?
#         if c["output"]["table"]:
#             t = c["output"]["table"]
#             tb = c["output"]["bag3d_table"]
#             c["output"]["bag3d_table"] = tb + "_border_ahn3"
#         else:
#             c["output"]["schema"] = "public"
#             c["output"]["table"] = "heights_border_ahn3"
#             c["output"]["bag3d_table"] = "bag3d_border_ahn3"


def write_yml(yml, file):
    """Write YAML config to file"""
    try:
        stream = open(file, "w")
        yaml.safe_dump(yml, stream)
    except FileNotFoundError as e:
        logging.exception("Config file not found at %s", file)
        exit(1)


def get_border_tiles(conn, tbl_schema, border_table, tbl_tile):
    """Get the border tile names as a list"""
    
    query = psycopg2.sql.SQL("""
    SELECT
        {tile}
    FROM
        {schema}.{border_table};
    """).format(
        tile=psycopg2.sql.Identifier(tbl_tile),
        schema=psycopg2.sql.Identifier(tbl_schema),
        border_table=psycopg2.sql.Identifier(border_table)
        )
    
    with conn:
        with conn.cursor() as cur:
            cur.execute(query)
            r = cur.fetchall()
            tiles = [row[0] for row in r]
    
    return tiles


def get_non_border_tiles(conn, tbl_schema, tbl_name, border_table, tbl_tile):
    """Get the non-border tile names as a list"""
    query = psycopg2.sql.SQL("""
    SELECT
        a.a_bladnr AS bladnr
    FROM
        (
            SELECT
                a.{tile} a_bladnr,
                b.{tile} b_bladnr
            FROM
                {schema}.{table} a
            LEFT JOIN {schema}.{border_table} b ON
                a.{tile} = b.{tile}
        ) a
    WHERE
        a.b_bladnr IS NULL;
    """).format(
        tile=psycopg2.sql.Identifier(tbl_tile),
        schema=psycopg2.sql.Identifier(tbl_schema),
        table=psycopg2.sql.Identifier(tbl_name),
        border_table=psycopg2.sql.Identifier(border_table)
        )
    
    with conn:
        with conn.cursor() as cur:
            cur.execute(query)
            r = cur.fetchall()
            tiles = [row[0] for row in r]
            
    return tiles


def main(config):
    conf_file = path.abspath(config['config']['in'])
    conf_rest = path.abspath(config['config']['out_rest'])
    conf_border_ahn2 = path.abspath(config['config']['out_border_ahn2'])
    conf_border_ahn3 = path.abspath(config['config']['out_border_ahn3'])
    a2_dir = path.abspath(config['ahn2']['dir'])
    a3_dir = path.abspath(config['ahn3']['dir'])
    
    tbl_schema = config['tile_index']['schema']
    tbl_name = config['tile_index']['table']['name']
    tbl_tile = config['tile_index']['table']['tile']
    border_table = config['tile_index']['border_table']
    
    try:
        conn = psycopg2.connect(
            "dbname=%s host=%s port=%s user=%s" %
            (config['db']['dbname'], config['db']['host'],
             config['db']['port'], config['db']['user']))
        logging.debug("Opened database successfully")
    except BaseException as e:
        logging.exception("I'm unable to connect to the database. Exiting function.", e)
    
    t_border = get_border_tiles(conn, tbl_schema, border_table, tbl_tile)
    t_rest = get_non_border_tiles(conn, tbl_schema, tbl_name, border_table,
                                 tbl_tile)
    
    conf_yml = parse_yml(conf_file)
    #TODO: user batch3dfierapp.parse_config_yml() instead
    bt = set(conf_yml["input_polygons"]["tile_list"]).intersection(set(t_border))
    if len(bt) > 0:
        w = "Tiles %s are on the border of AHN3 and they might be missing points" % bt
        warnings.warn(w, UserWarning)
        t_border = copy.deepcopy(list(bt))
        rt = list(set(conf_yml["input_polygons"]["tile_list"]).intersection(set(t_rest)))
        t_rest = copy.deepcopy(rt)
        del rt, bt

    
    yml_rest = update_yml(conf_yml, t_rest)
    # re-configure the border tiles with AHN2 only
    yml_border_ahn2 = update_yml(conf_yml, t_border, ahn_version=2, ahn_dir=a2_dir,
                            border_table=border_table)
    # and with AHN3 only
    yml_border_ahn3 = update_yml(conf_yml, t_border, ahn_version=3, ahn_dir=a3_dir,
                            border_table=border_table)
    
    write_yml(yml_rest, conf_rest)
    write_yml(yml_border_ahn2, conf_border_ahn2)
    write_yml(yml_border_ahn3, conf_border_ahn3)
    
    conn.close()


if __name__ == '__main__':
    main(config)
