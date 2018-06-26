#!/usr/bin/python3
# -*- coding: utf-8 -*-

"""Configure the batch3dfier processes for the tiles on AHN2 and AHN3 border"""

from os import path
import warnings
import copy
import re
import logging

import yaml
import psycopg2
from psycopg2 import sql

from bag3d.update import ahn

logger = logging.getLogger("config.border")


def create_border_table(conn, config, doexec=True):
    """Creates the table tile_index:elevation:border_table in the database
    
    The table tile_index:elevation:border_table a subset of tile index with 
    the tiles on the border of AHN3 coverage.
    AHN3 does not cover the whole Netherlands, AHN2 does. The tiles on the 
    border of AHN3 coverage only partially contain points, clipped at natural
    boundaries (eg a river). Therefore these tiles need to be identified and 
    processed separately.
    
    If border_table exists, it will drop it first.
    
    Parameters
    ----------
    conn : :py:class:`bag3d.config.db.db`
        Open connection
    config : dict
        bag3d configuration parameters
    
    Returns
    -------
    None
        Creates the table tile_index:elevation:border_table in the database
    """
    tbl_schema = sql.Identifier(config["tile_index"]['elevation']['schema'])
    tbl_name = sql.Identifier(config["tile_index"]['elevation']['table'])
    tbl_version = sql.Identifier(config["tile_index"]['elevation']['fields']['version'])
    tbl_geom = sql.Identifier(config["tile_index"]['elevation']['fields']['geometry'])
    border_table = sql.Identifier(config["tile_index"]['elevation']['border_table'])

    drop_q = sql.SQL("""
    DROP TABLE IF EXISTS {schema}.{border_table} CASCADE;
    """).format(schema=tbl_schema, border_table=border_table)
    logger.debug(conn.print_query(drop_q))
    
    create_q = sql.SQL("""
    CREATE TABLE {schema}.{border_table} AS
    WITH ahn2 AS (
        SELECT *
        FROM {schema}.{table}
        WHERE {version} = 2
    ),
    ahn3 AS (
        SELECT *
        FROM {schema}.{table}
        WHERE {version} = 3
    )
    SELECT DISTINCT ahn3.*
    FROM ahn3, ahn2
    WHERE st_touches(ahn3.{geom}, ahn2.{geom});
    """).format(
            schema=tbl_schema,
            table=tbl_name,
            version=tbl_version,
            geom=tbl_geom,
            border_table=border_table
            )
    logger.debug(conn.print_query(create_q))
    
    update_q = sql.SQL("""
    UPDATE {schema}.{border_table} SET {version} = 2;
    """).format(
        schema=tbl_schema,
        border_table=border_table,
        version=tbl_version
        )
    logger.debug(conn.print_query(update_q))

    if doexec:
        conn.sendQuery(drop_q)
        conn.sendQuery(create_q)
        conn.sendQuery(update_q)


def update_file_date(conn, config, ahn2_dir, ahn2_fp, doexec=True):
    """Update the file_date field in the border_tiles table
    
    Operates on the AHN-tile index in a BAG database. The result is a new AHN tile index
    with only those tiles that are on the border of the AHN3 and AHN2 version. These
    tiles are marked as AHN2 with AHN2 file creation date. 

    The border tiles only partially contain AHN3 data, therefore they need to be 
    extended with AHN2 data, resulting in a 3D BAG tile with mixed AHN2-3 heights.
    
    Parameters
    ----------
    conn : :py:class:`bag3d.config.db.db`
        Open connection
    config : dict
        bag3d configuration parameters
    ahn2_dir : str
        Path to the AHN2 files. Such as in input_elevation:dataset_dir
    ahn2_fp : str
        The filename pattern of AHN2 files. Such as in input_elevation:dataset_name
    
    Returns
    -------
    None
        Updates the tile_index:elevation:border_table in the database
        
    """
    
    tbl_schema = sql.Identifier(config["tile_index"]['elevation']['schema'])
    tbl_tile = sql.Identifier(config["tile_index"]['elevation']['fields']['unit_name'])
    tbl_version = sql.Identifier(config["tile_index"]['elevation']['fields']['version'])
    border_table = sql.Identifier(config["tile_index"]['elevation']['border_table'])
    a_date_pat = re.compile(r"(?<=\sfile creation day/year:).*",
                            flags=re.IGNORECASE & re.MULTILINE)
    corruptedfiles = []
    
    tile_q = sql.SQL("""
    SELECT {tile} FROM {schema}.{border_table};
    """).format(
        tile=tbl_tile,
        schema=tbl_schema,
        border_table=border_table
    )
    logger.debug(conn.print_query(tile_q))
    r = conn.getQuery(tile_q)
    tiles = [field[0].lower() for field in r]
    
    queries = sql.Composed('')
    for e, t in enumerate(tiles):
        d = ahn.get_file_date(ahn2_dir, ahn2_fp, t, a_date_pat, corruptedfiles)
        if d:
            date = d.isoformat()
            query = sql.SQL("""
            UPDATE {schema}.{border_table}
            SET file_date = {d}
            WHERE {tile} = {t};
            """).format(
                    schema=tbl_schema,
                    border_table=border_table,
                    d=psycopg2.sql.Literal(date),
                    tile=tbl_tile,
                    t=psycopg2.sql.Literal(t)
                )
            queries += query
        else:
            logger.debug("No file date for tile: %s", t)
            date = None
            query = sql.SQL("""
            UPDATE {schema}.{border_table}
            SET
            {version} = NULL,
            file_date = {d}
            WHERE {tile} = {t};
            """).format(
                    schema=tbl_schema,
                    border_table=border_table,
                    version = tbl_version,
                    d=psycopg2.sql.Literal(date),
                    tile=tbl_tile,
                    t=psycopg2.sql.Literal(t)
                )
            queries += query
    logger.debug(conn.print_query(queries))
    if doexec:
        conn.sendQuery(queries)


def update_output(cfg, ahn_version, ahn_dir, border_table):
    """Update the output parameters in the config file for processing border tiles
    
    Assumes that input_elevation:dataset_dir has at least 2 entries, one of
    them is the AHN2 directory.
    Updates the tile_index:elevation:table so that AHN2 tiles use the
    border_tiles index, and AHN3 tiles use the normal AHN tile index.
    
    Parameters
    ----------
    cfg : dict
        bag3d configuration parameters
    ahn_version : int
        Version of AHN
    ahn_dir : str
        Path to AHN files
    border_table : str
        Name of the border table
    
    Returns
    -------
    dict
        The updated configuration
    """
    sfx = "_border_ahn" + str(ahn_version)
    try:
        name_idx = cfg["input_elevation"]["dataset_dir"].index(ahn_dir)
        n = cfg["input_elevation"]["dataset_name"][name_idx]
        cfg["input_elevation"]["dataset_name"] = n
    except ValueError as e:
        logger.error("Cannot find %s in input_elevation:dataset_dir \
        of batch3dfier config", ahn_dir)
        raise
    # configure to use AHN2 only
    # schema is not expected to change for border_table
    if ahn_version == 2:
        cfg["tile_index"]["elevation"]["table"] = border_table
    
    cfg["input_elevation"]["dataset_dir"] = ahn_dir
    d = cfg["output"]["dir"]
    dname = path.join(path.dirname(d), path.basename(d) + sfx)
    cfg["output"]["dir"] = dname
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


def update_tile_list(config, tile_list, ahn_version=None, 
                     ahn_dir=None, border_table=None):
    """Update the tile_list in the config
    
    Parameters
    ----------
    config : dict
        bag3d configuration parameters
    tile_list : list
        List of tiles to process
    ahn_version : int
        Version of AHN
    ahn_dir : str
        Path to AHN files
    border_table : str
        Name of the border table
    
    Returns
    -------
    dict
        The updated configuration
    """
    c = copy.deepcopy(config)
    tl = list(set(tile_list).intersection(set(config["input_polygons"]["tile_list"])))
    c["input_polygons"]["tile_list"] = tl
    
    if ahn_version:
        c = update_output(c, ahn_version, ahn_dir, border_table)
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


def write_yml(yml, file):
    """Write YAML config to file"""
    try:
        stream = open(file, "w")
        yaml.safe_dump(yml, stream)
    except FileNotFoundError as e:
        logger.exception("Config file not found at %s", file)
        raise


def get_border_tiles(conn, tbl_schema, border_table, tbl_tile):
    """Get the border tile names as a list
    
    Parameters
    ----------
    conn : :py:class:`bag3d.config.db.db`
        Open connection
    tbl_schema : str
        The value of tile_index:elevation:schema
    border_table : str
        The value of tile_index:elevation:border_table
    tbl_tile : str
        The value of tile_index:elevation:fields:unit_name
    
    Returns
    -------
    list
        List of tile names that are on the border of AHN3 and AHN2 coverage
    """
    
    query = sql.SQL("""
    SELECT {tile} FROM {schema}.{border_table};
    """).format(
        tile=sql.Identifier(tbl_tile),
        schema=sql.Identifier(tbl_schema),
        border_table=sql.Identifier(border_table)
        )
    logger.debug(conn.print_query(query))
    r = [row[0] for row in conn.getQuery(query)]
    logger.debug("%s", r)
    return r


def get_non_border_tiles(conn, tbl_schema, tbl_name, border_table, tbl_tile):
    """Get the non-border tile names as a list
    
    Parameters
    ----------
    conn : :py:class:`bag3d.config.db.db`
        Open connection
    tbl_schema : str
        The value of tile_index:elevation:schema
    tbl_name : str
        The value of tile_index:elevation:table
    border_table : str
        The value of tile_index:elevation:border_table
    tbl_tile : str
        The value of tile_index:elevation:fields:unit_name
    
    Returns
    -------
    list
        List of tile names that are not on the border of AHN3 and AHN2 coverage
    """
    query = sql.SQL("""
    SELECT 
        sub.a_bladnr
    FROM
        (
            SELECT
                a.{tile} a_bladnr,
                b.{tile} b_bladnr
            FROM
                {schema}.{table} a
            LEFT JOIN {schema}.{border_table} b ON
                a.{tile} = b.{tile}
        ) sub
    WHERE 
        sub.b_bladnr IS NULL;
    """).format(
        tile=sql.Identifier(tbl_tile),
        schema=sql.Identifier(tbl_schema),
        table=sql.Identifier(tbl_name),
        border_table=sql.Identifier(border_table)
        )
    logger.debug(conn.print_query(query))
    r = [row[0] for row in conn.getQuery(query)]
    logger.debug("%s", r)
    return r


def process(conn, config, ahn3_dir, ahn2_dir, export=False):
    """Creates configurations for processing the border tiles
    
    For every configuration, updates config:in to the relevant path
    
    Parameters
    ----------
    conn : :py:class:`bag3d.config.db.db`
        Open connection
    config : dict
        bag3d configuration parameters
    ahn3_dir : str
        Path to the AHN3 files. Such as in input_elevation:dataset_dir
    ahn2_dir : str
        Path to the AHN2 files. Such as in input_elevation:dataset_dir
    export : bool
        Write the created configurations to files. The output directory is 
        where the original config file is located
    
    Returns
    -------
    list of dict
        List of the updated configurations in this order:
        1. Config for the non-border tiles
        2. Config for the border tiles with AHN2 data
        3. Config for the border tiles with AHN3 data
    """
    conf_rest = path.abspath(config['config']['out_rest'])
    conf_border_ahn2 = path.abspath(config['config']['out_border_ahn2'])
    conf_border_ahn3 = path.abspath(config['config']['out_border_ahn3'])
    a2_dir = path.abspath(ahn2_dir)
    a3_dir = path.abspath(ahn3_dir)
    tbl_schema = config["tile_index"]['elevation']['schema']
    tbl_name = config["tile_index"]['elevation']['table']
    tbl_tile = config["tile_index"]['elevation']['fields']['unit_name']
    border_table = config["tile_index"]['elevation']['border_table']

    t_border = get_border_tiles(conn, tbl_schema, border_table, tbl_tile)
    t_rest = get_non_border_tiles(conn, tbl_schema, tbl_name, border_table,
                                 tbl_tile)
    bt = set(config['tiles']).intersection(set(t_border))
    if len(bt) > 0:
        w = "Tiles %s are on the border of AHN3 and they might be missing points" % bt
        warnings.warn(w, UserWarning)
        t_border = copy.deepcopy(list(bt))
        rt = list(set(config['tiles']).intersection(set(t_rest)))
        t_rest = copy.deepcopy(rt)
        del rt, bt

    yml_rest = update_tile_list(config, t_rest)
    yml_rest["config"]["in"] = conf_rest
    # re-configure the border tiles with AHN2 only
    yml_border_ahn2 = update_tile_list(config, t_border, ahn_version=2, 
                                       ahn_dir=a2_dir,
                                       border_table=border_table)
    yml_border_ahn2["config"]["in"] = conf_border_ahn2
    # and with AHN3 only
    yml_border_ahn3 = update_tile_list(config, t_border, ahn_version=3, 
                                       ahn_dir=a3_dir,
                                       border_table=border_table)
    yml_border_ahn3["config"]["in"] = conf_border_ahn3
    
    if export:
        write_yml(yml_rest, conf_rest)
        write_yml(yml_border_ahn2, conf_border_ahn2)
        write_yml(yml_border_ahn3, conf_border_ahn3)
    return [yml_rest, yml_border_ahn2, yml_border_ahn3]
