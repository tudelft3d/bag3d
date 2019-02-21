# -*- coding: utf-8 -*-

"""Export the 3D BAG into files"""

import os
import datetime
import logging

from psycopg2 import sql

from bag3d.update import bag


logger = logging.getLogger("export")

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
    bag3d_table_q = sql.Identifier(config["output"]['bag3d_table'])
    
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
        FROM bagactueel.{bag3d})
    TO STDOUT
    WITH (FORMAT 'csv', HEADER TRUE, ENCODING 'utf-8', 
          FORCE_QUOTE (identificatie,gemeentecode,ahn_file_date,tile_id) )
    """).format(bag3d=bag3d_table_q)
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
    bag3d = config["output"]['bag3d_table']
    date = datetime.date.today().isoformat()
    x = "bag3d_{d}.gpkg".format(d=date)
    d = os.path.join(out_dir, "gpkg")
    os.makedirs(d, exist_ok=True)
    f = os.path.join(d, x)
    if conn.password:
        dns = "PG:'dbname={db} host={h} port={p} user={u} password={pw} \
        schemas=bagactueel tables={bag3d}'".format(db=conn.dbname,
                                                 h=conn.host,
                                                 p=conn.port,
                                                 pw=conn.password,
                                                 u=conn.user,
                                                 bag3d=bag3d)
    else:
        dns = "PG:'dbname={db} host={h} port={p} user={u} schemas=bagactueel \
        tables={bag3d}'".format(db=conn.dbname, h=conn.host, p=conn.port,
                                pw=conn.password, u=conn.user, bag3d=bag3d)
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
    bag3d = config["output"]['bag3d_table']
    
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
    f = os.path.join(postgis_dir, "bag3d_{d}.backup".format(d=date))
    tbl = "bagactueel.%s" % bag3d
    command = ["pg_dump", "--host", conn.host, "--port", conn.port,
               "--username", conn.user, "--no-password", "--format", 
               "custom", "--no-owner", "--compress", "7", "--encoding", 
               "UTF8", "--verbose", "--file", f, "--table", tbl, conn.dbname]
    logger.info("Exporting PostGIS backup")
    bag.run_subprocess(command, shell=True, doexec=doexec)
    compute_md5(f, postgis_dir)

