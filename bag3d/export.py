# -*- coding: utf-8 -*-

"""Export the 3D BAG into files"""


import os
import sys
from subprocess import run

import argparse
from psycopg2 import sql
import datetime
import logging

from batch3dfier.batch3dfierapp import parse_config


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



def csv():
    """Export into comma-separated-value"""
    
def gpkg():
    """Export into GeoPackage"""
    
def postgis():
    """Export as PostgreSQL backup file"""