# -*- coding: utf-8 -*-

"""Export the 3D BAG into files"""

import os
import datetime
import logging

from psycopg2 import sql

from bag3d.update import bag


logger = logging.getLogger(__name__)

def migrate(conn, config):
    """Migrate the 3D BAG from the staging area to production"""
    staging_schema = sql.Identifier(config["output"]["staging"]["schema"])
    staging_table = sql.Identifier(config["output"]["staging"]["bag3d_table"])
    prod_schema = sql.Identifier(config["output"]["production"]["schema"])
    prod_table = sql.Identifier(config["output"]["production"]["bag3d_table"])
    uniqueid_q = sql.Identifier(config['input_polygons']['footprints']['fields']['uniqueid'])

    query = sql.SQL("""
    CREATE SCHEMA IF NOT EXISTS {pr_s};
    """).format(pr_s=prod_schema)
    conn.sendQuery(query)

    query = sql.SQL("""
    DROP TABLE IF EXISTS {pr_s}.{pr_t} CASCADE;
    """).format(pr_s=prod_schema, pr_t=prod_table)
    conn.sendQuery(query)

    query = sql.SQL("""
    CREATE TABLE {pr_s}.{pr_t} AS SELECT * FROM {st_s}.{st_t};
    """).format(pr_s=prod_schema,pr_t=prod_table,st_s=staging_schema,st_t=staging_table)
    conn.sendQuery(query)

    idx = sql.Identifier(config["output"]["production"]["bag3d_table"] + "_identificatie_idx")
    query = sql.SQL("""
    CREATE INDEX {idx} ON {schema}.{bag3d} ({uniqueid});
    """).format(idx=idx, bag3d=prod_table, schema=prod_schema, uniqueid=uniqueid_q)
    conn.sendQuery(query)

    idx = sql.Identifier(config["output"]["production"]["bag3d_table"] + "_tile_id_idx")
    query = sql.SQL("""
    CREATE INDEX {idx} ON {schema}.{bag3d} (tile_id);
    """).format(idx=idx, bag3d=prod_table, schema=prod_schema)
    conn.sendQuery(query)

    idx = sql.Identifier(config["output"]["production"]["bag3d_table"] + "_valid_idx")
    query = sql.SQL("""
    CREATE INDEX {idx} ON {schema}.{bag3d} (height_valid);
    """).format(idx=idx, bag3d=prod_table, schema=prod_schema)
    conn.sendQuery(query)

    idx = sql.Identifier(config["output"]["production"]["bag3d_table"] + "_geovlak_idx")
    query = sql.SQL("""
    CREATE INDEX {idx} ON {schema}.{bag3d} USING GIST (geovlak);
    """).format(idx=idx, bag3d=prod_table, schema=prod_schema)
    conn.sendQuery(query)

    query = sql.SQL("""
    SELECT populate_geometry_columns('{schema}.{bag3d}'::regclass);
    """).format(bag3d=prod_table, schema=prod_schema)
    conn.sendQuery(query)

    query = sql.SQL("""
    COMMENT ON TABLE {schema}.{bag3d} IS 'The 3D BAG';
    """).format(bag3d=prod_table, schema=prod_schema)
    conn.sendQuery(query)


def compute_md5(file, d):
    """Compute the md5sum of a file"""
    filename = os.path.splitext(os.path.basename(file))
    md5_file = filename[0] + ".md5"
    cmd = ["md5sum", "--tag", file, ">", os.path.join(d, md5_file)]
    bag.run_subprocess(cmd, shell=True  )

def csv(conn, config, out_dir):
    """Export the 3DBAG table into a CSV file
    
    Parameters
    ----------
    conn : :py:class:`bag3d.config.db.db`
        Open connection
    config : dict
        Configuration
    out_dir : str
        Path to the output directory. The directory 'csv' will be created if 
        doesn't exist.
    """
    out_schema_q = sql.Identifier(config['output']['production']['schema'])
    bag3d_table_q = sql.Identifier(config['output']['production']['bag3d_table'])
    
    query = sql.SQL("""
    COPY (
        SELECT
            gid,
            identificatie,
            gemeentecode,
            "ground-0.00",
            "ground-0.10",
            "ground-0.20",
            "ground-0.30",
            "ground-0.40",
            "ground-0.50",
            "roof-0.25",
            "rmse-0.25",
            "roof-0.50",
            "rmse-0.50",
            "roof-0.75",
            "rmse-0.75",
            "roof-0.90",
            "rmse-0.90",
            "roof-0.95",
            "rmse-0.95",
            "roof-0.99",
            "rmse-0.99",
            roof_flat::int,
            nr_ground_pts,
            nr_roof_pts,
            ahn_file_date::date,
            ahn_version,
            height_valid::int,
            tile_id
        FROM {out_schema}.{bag3d})
    TO STDOUT
    WITH (FORMAT 'csv', HEADER TRUE, ENCODING 'utf-8', 
          FORCE_QUOTE (identificatie,gemeentecode,ahn_file_date,tile_id) )
    """).format(bag3d=bag3d_table_q, out_schema=out_schema_q)
    logger.debug(conn.print_query(query))
    
    date = datetime.date.today().isoformat()
    x = "bag3d_{d}.csv".format(d=date)
    d = os.path.join(out_dir, "csv")
    os.makedirs(d, exist_ok=True)
    csv_out = os.path.join(d, x)
    with open(csv_out, "w") as c_out:
        with conn.conn.cursor() as cur:
            logger.info("Exporting CSV")
            cur.copy_expert(query, c_out)
    compute_md5(csv_out, d)


def gpkg(conn, config, out_dir, doexec=True):
    """Export into GeoPackage
    
    Parameters
    ----------
    conn : :py:class:`bag3d.config.db.db`
        Open connection
    config : dict
        Configuration
    out_dir : str
        Path to the output directory. The directory 'csv' will be created if 
        doesn't exist.
    """
    schema = config['output']['production']['schema']
    bag3d = config['output']['production']['bag3d_table']
    date = datetime.date.today().isoformat()
    x = "bag3d_{d}.gpkg".format(d=date)
    d = os.path.join(out_dir, "gpkg")
    os.makedirs(d, exist_ok=True)
    f = os.path.join(d, x)
    if conn.password:
        dns = "PG:'dbname={db} host={h} port={p} user={u} password={pw} \
        schemas={schema} tables={bag3d}'".format(db=conn.dbname,
                                                 h=conn.host,
                                                 p=conn.port,
                                                 pw=conn.password,
                                                 u=conn.user,
                                                 schema=schema,
                                                 bag3d=bag3d)
    else:
        dns = "PG:'dbname={db} host={h} port={p} user={u} schemas={schema} \
        tables={bag3d}'".format(db=conn.dbname, h=conn.host, p=conn.port,
                                pw=conn.password, u=conn.user, schema=schema, bag3d=bag3d)
    command = ["ogr2ogr", "-f", "GPKG", f, dns]
    logger.info("Exporting GPKG")
    bag.run_subprocess(command, shell=True, doexec=doexec)
    compute_md5(f, d)
    
def postgis(conn, config, out_dir, doexec=True):
    """Export as PostgreSQL backup file
    
    For example the backup can be restored as:
    
    .. code-block:: sh
    
        createdb <db>
        psql -d <db> -c 'create extension postgis;'
        pg_restore --no-owner --no-privileges -h <host> -U <user> -d <db> -w bagactueel_schema.backup
        pg_restore --no-owner --no-privileges -j 2 --clean -h <host> -U <user> -d <db> -w <bag3d backup>.backup

    Parameters
    ----------
    conn : :py:class:`bag3d.config.db.db`
        Open connection
    config : dict
        Configuration
    out_dir : str
        Path to the output directory. The directory 'csv' will be created if 
        doesn't exist.
    """
    schema = config['output']['production']['schema']
    bag3d = config['output']['production']['bag3d_table']
    
    date = datetime.date.today().isoformat()
    
    postgis_dir = os.path.join(out_dir, "postgis")
    os.makedirs(postgis_dir, exist_ok=True)
    # PostGIS schema (required because of the pandstatus custom data type)
    f = os.path.join(postgis_dir,"bagactueel_schema.backup")
    command = ["pg_dump", "--host", conn.host, "--port", conn.port,
               "--username", conn.user, "--no-password", "--format", 
               "custom", "--no-owner", "--compress", "7", "--encoding", 
               "UTF8", "--verbose", "--schema-only", "--schema", "bagactueel",
                "--file", f, conn.dbname]
    bag.run_subprocess(command, shell=True, doexec=doexec)
    compute_md5(f, postgis_dir)
    
    # The 3D BAG (building heights + footprint geom)s
    f = os.path.join(postgis_dir, 'bag3d_{d}.backup'.format(d=date))
    tbl = '%s.%s' % (schema, bag3d)
    command = ["pg_dump", "--host", conn.host, "--port", conn.port,
               "--username", conn.user, "--no-password", "--format", 
               "custom", "--no-owner", "--compress", "7", "--encoding", 
               "UTF8", "--verbose", "--file", f, "--table", tbl, conn.dbname]
    logger.info("Exporting PostGIS backup")
    bag.run_subprocess(command, shell=True, doexec=doexec)
    compute_md5(f, postgis_dir)

