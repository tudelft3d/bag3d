# -*- coding: utf-8 -*-

"""Import batch3dfier output into the database"""

import os
import sys
from subprocess import run

import argparse
import psycopg2
from psycopg2 import sql
import datetime
import logging

from bag3d.update import bag

logger = logging.getLogger('import')


def create_heights_table(conn, schema, table):
    """Create a postgres table that can store the content of 3dfier's CSV-BUILDINGS-MULTIPLE output
    
    Note
    ----
    The 'id' field is set to numeric because the data type of the 
    BAG 'identificatie' field in the NLExtract changes between extracts.
    
    Parameters
    ----------
    conn : :py:class:`bag3d.config.db.db`
        Open connection
    schema : string
        Name of the schema where to create the table
    table : string
        Name of the new table
    
    Raises
    ------
    Exception
        Raises every exception
    
    Returns
    -------
    boolean
        True on success
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
        "rmse-0.00" real,
        "roof-0.10" real,
        "rmse-0.10" real,
        "roof-0.25" real,
        "rmse-0.25" real,
        "roof-0.50" real,
        "rmse-0.50" real,
        "roof-0.75" real,
        "rmse-0.75" real,
        "roof-0.90" real,
        "rmse-0.90" real,
        "roof-0.95" real,
        "rmse-0.95" real,
        "roof-0.99" real,
        "rmse-0.99" real,
        roof_flat bool,
        nr_ground_pts int,
        nr_roof_pts int,
        ahn_file_date timestamptz,
        ahn_version smallint,
        tile_id text
        );
    """).format(schema=schema_q, table=table_q)
    logger.debug(conn.print_query(query))
    try:
        conn.sendQuery(query)
        return True
    except Exception as e:
        logger.exception(e)
        raise
        return False


def csv2db(conn, cfg, out_paths):
    """Create a table with multiple height info per BAG building footprint
    
    Note
    ----
    Only for 3dfier's CSV-BUILDINGS-MULTIPLE output. 
    Only Linux.
    Alter the CSV files by adding the ahn_file_date, ahn_version fields and values.
    
    Parameters
    ----------
    conn : :py:class:`bag3d.config.db.db`
        Open connection
    cfg: dict
        batch3dfier YAML config as returned by :meth:`bag3d.config.args.parse_config`
    out_paths: list of strings
        Paths of the CSV files
    """
    schema_pc_q = sql.Identifier(cfg['tile_index']['elevation']['schema'])
    table_pc_q = sql.Identifier(cfg['tile_index']['elevation']['table'])
    field_pc_unit_q = sql.Identifier(cfg['tile_index']['elevation']['fields']['unit_name'])
    
    schema_out_q = sql.Identifier(cfg['output']['schema'])
    table_out_q = sql.Identifier(cfg['output']['table'])
    
    table_idx = sql.Identifier(cfg['output']['schema'] + "_id_idx")
    a = create_heights_table(conn, cfg['output']['schema'], cfg['output']['table'])
    
    if a:
        tbl = ".".join([cfg['output']['schema'], cfg['output']['table']])
        with conn.conn.cursor() as cur:
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
                logger.debug(conn.print_query(query))
                resultset = conn.getQuery(query)
                logger.debug(resultset)
                # the AHN3 file creation date that is stored in the tile index
                try:
                    ahn_file_date = resultset[0][0].isoformat()
                    ahn_version = resultset[0][1]
                except IndexError:
                    ahn_file_date = -99.99
                    ahn_version = -99.99
                
                # Need to do some linux text-fu so that the whole csv file can
                # be imported with COPY instead of row-wise edit and import
                # in python (suuuper slow)
                # Watch out for trailing commas from the CSV (until #58 is fixed in 3dfier)
                cmd_add_ahn = "gawk -i inplace -F',' 'BEGIN { OFS = \",\" } {$27=\"%s,%s,%s\"; print}' %s" % (
                    ahn_file_date, 
                    ahn_version,
                    tile,
                    path)
                run(cmd_add_ahn, shell=True)
                cmd_header = "sed -i '1s/.*/id,ground-0.00,ground-0.10,ground-0.20,\
ground-0.30,ground-0.40,ground-0.50,roof-0.00,rmse-0.00,roof-0.10,rmse-0.10,\
roof-0.25,rmse-0.25,roof-0.50,rmse-0.50,roof-0.75,rmse-0.75,roof-0.90,rmse-0.90,\
roof-0.95,rmse-0.95,roof-0.99,rmse-0.99,roof_flat,nr_ground_pts,nr_roof_pts,\
ahn_file_date,ahn_version,tile_id/' %s" % path
                run(cmd_header, shell=True)
                
                with open(path, "r") as f_in:
                    next(f_in) # skip header
                    logger.debug(f_in)
                    cur.copy_from(f_in, tbl, sep=',', null='-99.99')
                        
        conn.sendQuery(
            sql.SQL("""CREATE INDEX IF NOT EXISTS {table}
                    ON {schema_q}.{table_q} (id);
                    """).format(schema_q=schema_out_q,
                                table_q=table_out_q,
                                table=table_idx)
        )
        conn.sendQuery(
            sql.SQL("""COMMENT ON TABLE {schema}.{table} IS
                    'Building heights generated with 3dfier.';
                    """).format(schema=schema_out_q,
                               table=table_out_q)
        )
    else:
        logger.error("csv2db: exit because create_heights_table returned False")
        raise


def create_bag3d_relations(conn, cfg):
    """Creates the necessary postgres tables and views for the 3D BAG
    
    Parameters
    ----------
    conn : :py:class:`bag3d.config.db.db`
        Open connection
    cfg: dict
        batch3dfier YAML config as returned by :meth:`bag3d.config.args.parse_config`
    """
    schema_q = sql.Identifier(cfg['input_polygons']['footprints']['schema'])
    bag_table_q = sql.Identifier(cfg['input_polygons']['footprints']['table'])
    uniqueid_q = sql.Identifier(cfg['input_polygons']['footprints']['fields']['uniqueid'])
    bag3d_table_q = sql.Identifier(cfg['output']['bag3d_table'])
    heights_table_q = sql.Identifier(cfg['output']['table'])
    
    drop_q = sql.SQL("DROP TABLE IF EXISTS {schema}.{bag3d} CASCADE;").format(
        bag3d=bag3d_table_q,
        schema=schema_q)
    conn.sendQuery(drop_q)
    
    query = sql.SQL("""
    CREATE TABLE {schema}.{bag3d} AS
    SELECT
        p.gid,
        p.identificatie::bigint,
        p.aanduidingrecordinactief,
        p.aanduidingrecordcorrectie,
        p.officieel,
        p.inonderzoek,
        p.documentnummer,
        p.documentdatum,
        make_date(p.bouwjaar::int, 1, 1) bouwjaar,
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
        h."rmse-0.00",
        h."roof-0.10",
        h."rmse-0.10",
        h."roof-0.25",
        h."rmse-0.25",
        h."roof-0.50",
        h."rmse-0.50",
        h."roof-0.75",
        h."rmse-0.75",
        h."roof-0.90",
        h."rmse-0.90",
        h."roof-0.95",
        h."rmse-0.95",
        h."roof-0.99",
        h."rmse-0.99",
        h.roof_flat,
        h.nr_ground_pts,
        h.nr_roof_pts,
        h.ahn_file_date,
        h.ahn_version,
        CASE
            WHEN make_date(p.bouwjaar::int, 1, 1) > h.ahn_file_date THEN FALSE
            WHEN h.nr_roof_pts = 0 THEN FALSE
            ELSE TRUE
        END AS height_valid,
        h.tile_id
    FROM {schema}.{bag} p
    INNER JOIN {schema}.{heights} h ON p.{uniqueid}::numeric = h.id;
    """).format(bag=bag_table_q, uniqueid=uniqueid_q,schema=schema_q,
                bag3d=bag3d_table_q, heights=heights_table_q)
    # the type of bagactueel.pand.identificatie can change between different 
    # BAG extracts (numeric or varchar)
    conn.sendQuery(query)
    
    idx = sql.Identifier(cfg['output']['bag3d_table'] + "_identificatie_idx")
    query = sql.SQL("""
    CREATE INDEX {idx} ON {schema}.{bag3d} ({uniqueid});
    """).format(idx=idx, bag3d=bag3d_table_q,schema=schema_q,uniqueid=uniqueid_q)
    conn.sendQuery(query)
    
    idx = sql.Identifier(cfg['output']['bag3d_table'] + "_tile_id_idx")
    query = sql.SQL("""
    CREATE INDEX {idx} ON {schema}.{bag3d} (tile_id);
    """).format(idx=idx, bag3d=bag3d_table_q,schema=schema_q)
    conn.sendQuery(query)
    
    idx = sql.Identifier(cfg['output']['bag3d_table'] + "_valid_idx")
    query = sql.SQL("""
    CREATE INDEX {idx} ON {schema}.{bag3d} (height_valid);
    """).format(idx=idx, bag3d=bag3d_table_q,schema=schema_q)
    conn.sendQuery(query)
    
    idx = sql.Identifier(cfg['output']['bag3d_table'] + "_geovlak_idx")
    query = sql.SQL("""
    CREATE INDEX {idx} ON {schema}.{bag3d} USING GIST (geovlak);
    """).format(idx=idx, bag3d=bag3d_table_q,schema=schema_q)
    conn.sendQuery(query)
    
    query = sql.SQL("""
    SELECT populate_geometry_columns('{schema}.{bag3d}'::regclass);
    """).format(bag3d=bag3d_table_q,schema=schema_q)
    conn.sendQuery(query)
    
    query = sql.SQL("""
    COMMENT ON TABLE {schema}.{bag3d} IS 'The 3D BAG';
    """).format(bag3d=bag3d_table_q,schema=schema_q)
    conn.sendQuery(query)
    
    query = sql.SQL("""
    DROP TABLE {schema}.{heights} CASCADE;
    """).format(heights=heights_table_q,schema=schema_q)
    conn.sendQuery(query)


def import_csv(conn, cfg):
    """Import the batch3dfier CSV output into the BAG database
    
    Parameters
    ----------
    conn : :py:class:`bag3d.config.db.db`
        Open connection
    cfg: dict
        batch3dfier YAML config as returned by :meth:`bag3d.config.args.parse_config`
    """
    
    # Get CSV files in dir
    for root, dir, filenames in os.walk(cfg['output']['dir'], topdown=True):
        csv_files = [f for f in filenames if os.path.splitext(f)[1].lower() == ".csv"]
        out_paths = [os.path.join(cfg['output']['dir'], f) for f in csv_files]
    try:
        logger.debug("out_paths: %s", out_paths)
        logger.info("There are {} CSV files in the directory".format(len(csv_files)))
    except UnboundLocalError as e:
        logger.exception("Couln't find any CSVs in %s", cfg['output']['dir'])
        raise
    csv2db(conn, cfg, out_paths)
    # TODO: add option for dropping the relations if exist
    create_bag3d_relations(conn, cfg)


def unite_border_tiles(conn, schema, border_ahn2, border_ahn3):
    """Unite the border tiles on the AHN2 and AHN3 border
    
    Creates a view 'bag3d_border_union' in schema. The view has the following
    condition on the imported CSV files:
    
    .. code-block:: sql
    
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
    
    Note
    ----
    BAG field name 'identificatie' is hardcoded
    
    Parameters
    ----------
    conn : :py:class:`bag3d.config.db.db`
        Open connection
    schema : str
        Value from output:schema
    border_ahn2 : str
        Value from output:bag3d_table in case of AHN2 border tiles
    border_ahn2 : str
        Value from output:bag3d_table in case of AHN3 border tiles
    
    Raises
    ------
    BaseException
        If cannot create the view
    
    Returns
    -------
    None
        Creates a view in database
    """
    
    query = sql.SQL("""
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
        schema=sql.Identifier(schema),
        border_ahn3=sql.Identifier(border_ahn3),
        border_ahn2=sql.Identifier(border_ahn2)
        )
    logger.debug(conn.print_query(query))
    try:
        conn.sendQuery(query)
    except BaseException as e:
        logger.exception(e)
        raise


def create_bag3d_table(conn, schema, name):
    """Unite the border tiles with the rest
    
    Note
    -----
    Persists and indexes the table 'bag3d' by uniting the border tiles with the 
    rest. 
    Drops the table 'bag3d' if exists before the operation.
    Drops the view 'bag3d_border_union'.
    
    Parameters
    ----------
    conn : :py:class:`bag3d.config.db.db`
        Open connection
    schema : str
        Value from output:schema
    name : str
        Name of the new table
    
    Raises
    ------
    BaseException
        If cannot create the table
    psycopg2.IntegrityError
        If the 'gid' field cannot be made PRIMARY KEY due to duplicate records
    
    Returns
    -------
    None
        Creates a table in database
    """
    drop_q = sql.SQL("DROP TABLE IF EXISTS {schema}.{bag3d} CASCADE;").format(
        bag3d=sql.Identifier(name),
        schema=sql.Identifier(schema))
    
    query_t = sql.SQL("""
    CREATE TABLE {schema}.{bag3d} AS
    SELECT *
    FROM {schema}.{bag3d_rest}
    WHERE ahn_version IS NOT NULL
    UNION
    SELECT *
    FROM {schema}.bag3d_border_union
    WHERE ahn_version IS NOT NULL;
    """).format(schema=sql.Identifier(schema), 
                bag3d=sql.Identifier(name),
                bag3d_rest=sql.Identifier(name+"_rest"))
    
    idx_name = name + "_geom_idx"
    query_i = sql.SQL("""
    CREATE INDEX {idx_name} ON {schema}.{bag3d} USING gist (geovlak);
    ALTER TABLE {schema}.{bag3d} ADD PRIMARY KEY (gid);
    COMMENT ON TABLE {schema}.{bag3d} IS 'The 3D BAG';
    """).format(schema=sql.Identifier(schema),
                bag3d=sql.Identifier(name),
                idx_name=sql.Identifier(idx_name))

    query_d = sql.SQL("""
    DROP VIEW {schema}.bag3d_border_union;
    """).format(schema=sql.Identifier(schema))
    
    # TODO: drop *border* tables
    try:
        logger.debug(conn.print_query(drop_q))
        conn.sendQuery(drop_q)
        logger.debug(conn.print_query(query_t))
        conn.sendQuery(query_t)
        logger.debug(conn.print_query(query_i))
        conn.sendQuery(query_i)
        logger.debug(conn.print_query(query_d))
        conn.sendQuery(query_d)
    except psycopg2.IntegrityError as e:
        logger.exception("There are overlapping footprints in the border and non-border tiles, possibly because some tiles were processed in a batch where they do not belong.")
        logger.exception(e)
        raise
    except BaseException as e:
        logger.exception(e)
        raise
