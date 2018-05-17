# -*- coding: utf-8 -*-

"""Import batch3dfier output into the database"""

import os
import sys
from subprocess import run

import argparse
from psycopg2 import sql
import datetime
import logging

from batch3dfier.batch3dfierapp import parse_config_yaml


def create_heights_table(db, schema, table):
    """Create a postgres table that can store the content of 3dfier CSV-BUILDINGS-MULTIPLE
    
    Note
    ----
    The 'id' field is set to numeric because of the BAG 'identificatie' field.
    """

    schema_q = sql.Identifier(schema)
    table_q = sql.Identifier(table)
    query = sql.SQL("""
    CREATE TABLE IF NOT EXISTS {schema}.{table} (
        id bigint,
        "ground-0.00" real,
        "ground-0.10" real,
        "ground-0.20" real,
        "ground-0.30" real,
        "ground-0.40" real,
        "ground-0.50" real,
        "roof-0.00" real,
        "roof-0.10" real,
        "roof-0.25" real,
        "roof-0.50" real,
        "roof-0.75" real,
        "roof-0.90" real,
        "roof-0.95" real,
        "roof-0.99" real,
        ahn_file_date timestamptz,
        ahn_version smallint
        );
    """).format(schema=schema_q, table=table_q)
    try:
        db.sendQuery(query)
        logging.debug("Created heights table")
        return True
    except:
        logging.error(query.as_string(db.conn))
        sys.exit(1)
        return False


def csv2db(cfg, out_paths):
    """Create a table with multiple height info per BAG building footprint
    
    Note
    ----
    Only for 3dfier's CSV-BUILDINGS-MULTIPLE output. 
    Only works when the AHN3 and BAG tiles are the same (same size and identifier). 
    Only Linux.
    Alter the CSV files by adding the ahn_file_date, ahn_version fields and values.
    
    Parameters
    ----------
    db : db Class instance
    cfg: dict
        batch3dfier YAML config (output by parse_config_yaml() )
    args_in: dict
        batch3dfier command line arguments
    out_paths: list of strings
        Paths of the CSV files
    """
    db = cfg["dbase"]
    
    schema_pc_q = sql.Identifier(cfg['elevation']['schema'])
    table_pc_q = sql.Identifier(cfg['elevation']['table'])
    field_pc_unit_q = sql.Identifier(cfg['elevation']['fields']['unit_name'])
    
    schema_out_q = sql.Identifier(cfg['out_schema'])
    table_out_q = sql.Identifier(cfg['out_table'])
    
    table_idx = sql.Identifier(cfg['out_schema'] + "_id_idx")
    
    a = create_heights_table(db, cfg['out_schema'], cfg['out_table'])
    
    if a:
        with db.conn:
            with db.conn.cursor() as cur:
                tbl = ".".join([cfg['out_schema'], cfg['out_table']])
                for path in out_paths:
                    csv_file = os.path.split(path)[1]
                    fname = os.path.splitext(csv_file)[0]
                    tile = fname.replace(cfg['prefix_tile_footprint'], '', 1)
                    tile_q = sql.Literal(tile)
                    
                    query = sql.SQL("""SELECT file_date, ahn_version
                                        FROM {schema}.{table}
                                        WHERE {unit_name} = {tile};
                                    """).format(schema=schema_pc_q,
                                               table=table_pc_q,
                                               unit_name=field_pc_unit_q,
                                               tile=tile_q)
                    cur.execute(query)
                    resultset = cur.fetchall()
                    # the AHN3 file creation date that is stored in the tile index
                    if resultset[0][0] and resultset[0][1]:
                        ahn_file_date = resultset[0][0].isoformat()
                        ahn_version = resultset[0][1]
                    else:
                        ahn_file_date = -99.99
                        ahn_version = -99.99
                    
                    # Need to do some linux text-fu so that the whole csv file can
                    # be imported with COPY instead of row-wise edit and import
                    # in python (suuuper slow)
                    # Watch out for trailing commas from the CSV (until #58 is fixed in 3dfier)
                    cmd_add_ahn = "gawk -i inplace -F',' 'BEGIN { OFS = \",\" } {$16=\"%s,%s\"; print}' %s" % (
                        ahn_file_date, 
                        ahn_version,
                        path)
                    run(cmd_add_ahn, shell=True)
                    cmd_header = "sed -i '1s/.*/id,ground-0.00,ground-0.10,ground-0.20,\
ground-0.30,ground-0.40,ground-0.50,roof-0.00,roof-0.10,\
roof-0.25,roof-0.50,roof-0.75,roof-0.90,roof-0.95,roof-0.99,\
ahn_file_date,ahn_version/' %s" % path
                    run(cmd_header, shell=True)
                    
                    with open(path, "r") as f_in:
                        next(f_in) # skip header
                        cur.copy_from(f_in, tbl, sep=',', null='-99.99')
                        
        db.sendQuery(
            sql.SQL("""CREATE INDEX IF NOT EXISTS {table}
                    ON {schema_q}.{table_q} (id);
                    """).format(schema_q=schema_out_q,
                                table_q=table_out_q,
                                table=table_idx)
        )
        db.sendQuery(
            sql.SQL("""COMMENT ON TABLE {schema}.{table} IS
                    'Building heights generated with 3dfier.';
                    """).format(schema=schema_out_q,
                               table=table_out_q)
        )
    else:
        logging.error("csv2db: exit because create_heights_table returned False")
        sys.exit(1)


def create_bag3d_relations(cfg):
    """Creates the necessary postgres tables and views for the 3D BAG"""
    db = cfg["dbase"]
    
    bag3d_table_q = sql.Identifier(cfg['bag3d_table'])
    heights_table_q = sql.Identifier(cfg['out_table'])
    
    query = sql.SQL("""
    CREATE TABLE bagactueel.{bag3d} AS
    SELECT
        p.gid,
        p.identificatie::bigint,
        p.aanduidingrecordinactief,
        p.aanduidingrecordcorrectie,
        p.officieel,
        p.inonderzoek,
        p.documentnummer,
        p.documentdatum,
        p.pandstatus,
        p.bouwjaar,
        p.begindatumtijdvakgeldigheid,
        p.einddatumtijdvakgeldigheid,
        p.geovlak,
        h."ground-0.00",
        h."ground-0.10",
        h."ground-0.20",
        h."ground-0.30",
        h."ground-0.40",
        h."ground-0.50",
        h."roof-0.00",
        h."roof-0.10",
        h."roof-0.25",
        h."roof-0.50",
        h."roof-0.75",
        h."roof-0.90",
        h."roof-0.95",
        h."roof-0.99",
        h.ahn_file_date,
        h.ahn_version
    FROM bagactueel.pandactueelbestaand p
    INNER JOIN bagactueel.{heights} h ON p.identificatie::numeric = h.id;
    """).format(bag3d=bag3d_table_q, heights=heights_table_q)
    # the type of bagactueel.pand.identificatie can change between different 
    # BAG extracts (numeric or varchar)
    db.sendQuery(query)
    
    idx = sql.Identifier(cfg['bag3d_table'] + "_identificatie_idx")
    query = sql.SQL("""
    CREATE INDEX {idx} ON bagactueel.{bag3d} (identificatie);
    """).format(idx=idx, bag3d=bag3d_table_q)
    db.sendQuery(query)
    
    idx = sql.Identifier(cfg['bag3d_table'] + "_geovlak_idx")
    query = sql.SQL("""
    CREATE INDEX {idx} ON bagactueel.{bag3d} USING GIST (geovlak);
    """).format(idx=idx, bag3d=bag3d_table_q)
    db.sendQuery(query)
    
    query = sql.SQL("""
    SELECT populate_geometry_columns('bagactueel.{bag3d}'::regclass);
    """).format(bag3d=bag3d_table_q)
    db.sendQuery(query)
    
    query = sql.SQL("""
    COMMENT ON TABLE bagactueel.{bag3d} IS 'The 3D BAG';
    """).format(bag3d=bag3d_table_q)
    db.sendQuery(query)
    
    query = sql.SQL("""
    DROP TABLE bagactueel.{heights} CASCADE;
    """).format(heights=heights_table_q)
    db.sendQuery(query)
    
    viewname = sql.Identifier(cfg['bag3d_table'] + "_valid_height")
    query = sql.SQL("""
    CREATE OR REPLACE VIEW bagactueel.{viewname} AS
    SELECT *
    FROM bagactueel.{bag3d}
    WHERE bouwjaar <= date_part('YEAR', ahn_file_date) 
    AND begindatumtijdvakgeldigheid < ahn_file_date;
    """).format(bag3d=bag3d_table_q, viewname=viewname)
    db.sendQuery(query)
    
    query = sql.SQL("""
    COMMENT ON VIEW bagactueel.{viewname} IS \
    'The BAG footprints where the building was built before the AHN3 was created';
    """).format(viewname=viewname)
    db.sendQuery(query)


def run(args_in, cfg):
    """Import the batch3dfier CSV output into the BAG database"""
    
    # Get CSV files in dir
    for root, dir, filenames in os.walk(args_in['csv_dir'], topdown=True):
        csv_files = [f for f in filenames if os.path.splitext(f)[1].lower() == ".csv"]
        out_paths = [os.path.join(args_in['csv_dir'], f) for f in csv_files]
    try:
        logging.debug("out_paths: %s", out_paths)
        logging.info("There are {} CSV files in the directory".format(len(csv_files)))
    except UnboundLocalError as e:
        logging.exception("Couln't find any CSVs in %s", args_in['csv_dir'])
        sys.exit(1)

    csv2db(cfg, out_paths)
    
    # TODO: add option for dropping the relations if exist
    create_bag3d_relations(cfg)


# --------------------- Unite tiles

import psycopg2, psycopg2.sql


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


def unite_tiles():
    """Unite border tiles with the rest"""


def unite_border_tiles(conn, schema, border_ahn2, border_ahn3):
    """Unite the tiles on the AHN2 and AHN3 border"""
    
    query = psycopg2.sql.SQL("""
    CREATE OR REPLACE VIEW {schema}.bag3d_border_union AS
    WITH border_ahn3_notnull AS(
        SELECT
            a.*
        FROM
            {schema}.{border_ahn3} a
        WHERE
            a."ground-0.00" IS NOT NULL
            AND a."ground-0.10" IS NOT NULL
            AND a."ground-0.20" IS NOT NULL
            AND a."ground-0.30" IS NOT NULL
            AND a."ground-0.40" IS NOT NULL
            AND a."ground-0.50" IS NOT NULL
            AND a."roof-0.00" IS NOT NULL
            AND a."roof-0.10" IS NOT NULL
            AND a."roof-0.25" IS NOT NULL
            AND a."roof-0.50" IS NOT NULL
            AND a."roof-0.75" IS NOT NULL
            AND a."roof-0.90" IS NOT NULL
            AND a."roof-0.95" IS NOT NULL
            AND a."roof-0.99" IS NOT NULL
    ),
    border_ahn2_id AS(
        SELECT
            ARRAY_AGG( a.identificatie ) identificatie
        FROM
            (
                SELECT
                    identificatie
                FROM
                    {schema}.{border_ahn2}
            EXCEPT SELECT
                    identificatie
                FROM
                    border_ahn3_notnull
            ) a
    ) SELECT
        *
    FROM
        border_ahn3_notnull
    UNION SELECT
        a.*
    FROM
        {schema}.{border_ahn2} a,
        border_ahn2_id b
    WHERE
        a.identificatie = ANY(b.identificatie)
    ;
    """).format(
        schema=psycopg2.sql.Identifier(schema),
        border_ahn3=psycopg2.sql.Identifier(border_ahn3),
        border_ahn2=psycopg2.sql.Identifier(border_ahn2)
        )
    
    with conn:
        with conn.cursor() as cur:
            cur.execute(query)


def create_bag3d_table(conn, schema):
    """Unite the border tiles with the rest"""
    
    query = psycopg2.sql.SQL("""
    CREATE TABLE {schema}.bag3d AS
    SELECT *
    FROM {schema}.bag3d_rest
    UNION
    SELECT *
    FROM {schema}.bag3d_border_union;
    
    CREATE INDEX bag3d_geom_idx ON {schema}.bag3d USING gist (geovlak);
    ALTER TABLE {schema}.bag3d ADD PRIMARY KEY (gid);
    COMMENT ON TABLE {schema}.bag3d IS 'The 3D BAG';
    
    DROP VIEW {schema}.bag3d_border_union;
    """).format(schema=psycopg2.sql.Identifier(schema))
    
    with conn:
        with conn.cursor() as cur:
            cur.execute(query)
