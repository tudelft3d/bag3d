#!/usr/bin/python3
# -*- coding: utf-8 -*-

"""Generate a 3D BAG data set.

It processes the CSV output from 3dfier (CSV-BUILDINGS-MULTIPLE) and imports
the data back to the database. Finally, combines the BAG building (pand)
footprint geometry and attributes with the height values from the CSV and info
about the respective AHN tile (file date and AHN version). The result is the
<bag schema>.bag3d table containing all the mentioned fields, therefore it
duplicates the BAG pand table.
Only works when the AHN3 and BAG tiles have the same size and identifier. That
is, because 3dfier outputs a CSV per BAG tile, then the bag3d module assigns
a single AHN date and version to the whole CSV, based on the tile ID that is 
part of the CSV file name. This ID should have a match among the AHN tiles.
Works in Linux only, due to the use of gawk and sed.
"""

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


def combine_border_tiles():
    """Combine the border tiles from AHN2 and AHN3"""
    return None


def export_csv(cur, csv_out, cfg):
    """Export the 3DBAG table into a CSV file"""
    
    bag3d_table_q = sql.Identifier(cfg['bag3d_table'])
    
    query = sql.SQL("""COPY (
    SELECT
        gid,
        identificatie,
        aanduidingrecordinactief,
        aanduidingrecordcorrectie,
        officieel,
        inonderzoek,
        documentnummer,
        documentdatum,
        pandstatus,
        bouwjaar,
        begindatumtijdvakgeldigheid,
        einddatumtijdvakgeldigheid,
        "ground-0.00",
        "ground-0.10",
        "ground-0.20",
        "ground-0.30",
        "ground-0.40",
        "ground-0.50",
        "roof-0.00",
        "roof-0.10",
        "roof-0.25",
        "roof-0.50",
        "roof-0.75",
        "roof-0.90",
        "roof-0.95",
        "roof-0.99",
        ahn_file_date,
        ahn_version
    FROM bagactueel.{bag3d})
    TO STDOUT
    WITH (FORMAT 'csv', HEADER TRUE, ENCODING 'utf-8')
    """).format(bag3d=bag3d_table_q)
    
    with open(csv_out, "w") as c_out:
        cur.copy_expert(query, c_out)


def export_bag3d(cfg, out_dir):
    """Export and prepare the 3D BAG in various formats
    
    PostGIS dump is restored as:
    
    createdb <db>
    psql -d <db> -c 'create extension postgis;'
    
    pg_restore \
    --no-owner \
    --no-privileges \
    -h <host> \
    -U <user> \
    -d <db> \
    -w bagactueel_schema.backup
    
    pg_restore \
    --no-owner \
    --no-privileges \
    -j 2 \
    --clean \
    -h <host> \
    -U <user> \
    -d <db> \
    -w bag3d_30-12-2017.backup
    
    """
    db = cfg["dbase"]
    bag3d = cfg['bag3d_table']
    
    date = datetime.date.today().isoformat()
    
    postgis_dir = os.path.join(out_dir, "postgis")
    # PostGIS schema (required because of the pandstatus custom data type)
    command = "pg_dump \
--host {h} \
--port {p} \
--username {u} \
--no-password \
--format custom \
--no-owner \
--compress 7 \
--encoding UTF8 \
--verbose \
--schema-only \
--schema bagactueel \
--file {f} \
bag".format(h=db.host,
            p=db.port,
            u=db.user,
            f=os.path.join(postgis_dir,"bagactueel_schema.backup"))
    run(command, shell=True)
    
    # The 3D BAG (building heights + footprint geom)
    x =  "bag3d_{d}.backup".format(d=date)
    command = "pg_dump \
--host {h} \
--port {p} \
--username {u} \
--no-password \
--format custom \
--no-owner \
--compress 7 \
--encoding UTF8 \
--verbose \
--file {f} \
--table bagactueel.{bag3d} \
bag".format(h=db.host,
            p=db.port,
            u=db.user,
            f=os.path.join(postgis_dir, x),
            bag3d=bag3d)
    run(command, shell=True)
    
    # Create GeoPackage
    x = "bag3d_{d}.gpkg".format(d=date)
    f = os.path.join(out_dir, "gpkg", x)
    command = "ogr2ogr -f GPKG {f} \
    PG:'dbname={db} \
    host={h} \
    user={u} \
    password={pw} \
    schemas=bagactueel tables={bag3d}'".format(f=f,
                                             db=db.dbname,
                                             h=db.host,
                                             pw=db.password,
                                             u=db.user,
                                             bag3d=bag3d)
    run(command, shell=True)
    
    # CSV
    x = "bag3d_{d}.csv".format(d=date)
    csv_out = os.path.join(out_dir, "csv", x)
    with db.conn:
        with db.conn.cursor() as cur:
            export_csv(cur, csv_out, cfg)


def main():
    """Main function
    
    !!! The script processes ALL csv files in the given directory !!!

    Creates the table if doesn't exists.
    
    Example:
    
    $ csv2db.py -d /some/directory/with/CSVs -s bagactueel -t heights -rm 
    """
    
    parser = argparse.ArgumentParser(description="Copy CSV-BUILDINGS-MULTIPLE files to PostgreSQL table")
    parser.add_argument(
        "-d",
        help="Directory with CSV files")
    parser.add_argument(
        "-c",
        help="batch3dfier config file")
    parser.add_argument(
        "-o",
        help="Output directory")
    parser.add_argument(
        "--no-export",
        action="store_false",
        dest="e",
        help="Do not export the 3D BAG into CSV, GPKG, PostgreSQL")
    parser.add_argument(
        "--del-csv",
        action="store_true",
        dest="rm",
        help="Remove CSV files from disk after import")
    parser.add_argument(
        "--keep-csv",
        action="store_false",
        dest="rm",
        help="Keep CSV files from disk after import (default)")
    parser.set_defaults(rm=False)
    parser.set_defaults(e=True)

    args = parser.parse_args()
    args_in = {}
    args_in['csv_dir'] = os.path.abspath(args.d)
    # TODO: detect missing -o and use that instead of --no-export
    args_in['out_dir'] = os.path.abspath(args.o)
    args_in['rm'] = args.rm
    args_in["cfg_file"] = args.c
    args_in['export'] = args.e
    
    cfg = parse_config_yaml(args_in)

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
    
    if args_in['export']:
        export_bag3d(cfg, args_in['out_dir'])
    
    if args_in['rm']:
        p = os.path.join(args_in['csv_dir'], "*.csv")
        cmd = " ".join("rm", p)
        run(cmd, shell=True)
    
    cfg['dbase'].close()
    
    # report how many files were created and how many tiles are there


if __name__ == '__main__':
    main()