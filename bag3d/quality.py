# -*- coding: utf-8 -*-

"""Quality control for the 3D BAG"""

import logging
import json
from math import sqrt

from psycopg2 import sql
from psycopg2.extras import Json
import numpy as np
from rasterstats import zonal_stats

from bag3d.config import border

logger = logging.getLogger(__name__)


def create_quality_views(conn, cfg):
    """Create the views that are used for quality control
    
    Parameters
    ----------
    conn : :py:class:`bag3d.config.db.db`
        Open connection
    cfg: dict
        batch3dfier YAML config as returned by :meth:`bag3d.config.args.parse_config`
    
    Retruns
    -------
    dict
        The configuration with the names of the created views (in quality:views)
    
    Raises
    ------
    BaseException
        If cannot create the table
    """
    config = cfg
    
    name = config["output"]["bag3d_table"]
    name_q = sql.Identifier(name)
    config["quality"]["views"] = {}
    config["quality"]["views"]["valid"] = name + "_valid"
    config["quality"]["views"]["invalid_height"] = name + "_invalid_height"
    config["quality"]["views"]["missing_ground"] = name + "_missing_ground"
    config["quality"]["views"]["missing_roof"] = name + "_missing_roof"
    config["quality"]["views"]["sample"] = name + "_sample"
    
    viewname = sql.Identifier(config["quality"]["views"]["valid"])
    query_valid = sql.SQL("""
    CREATE TABLE IF NOT EXISTS bagactueel.{viewname} AS
    SELECT *
    FROM bagactueel.{bag3d}
    WHERE bouwjaar <= date_part('YEAR', ahn_file_date)
    AND pandstatus <> 'Bouwvergunning verleend'::bagactueel.pandstatus
    AND pandstatus <> 'Bouw gestart'::bagactueel.pandstatus
    AND pandstatus <> 'Niet gerealiseerd pand'::bagactueel.pandstatus
    AND pandstatus <> 'Pand gesloopt'::bagactueel.pandstatus;
    """).format(bag3d=name_q, viewname=viewname)
    
    viewname = sql.Identifier(config["quality"]["views"]["invalid_height"])
    query_v = sql.SQL("""
    CREATE OR REPLACE VIEW bagactueel.{viewname} AS
    SELECT *
    FROM bagactueel.{bag3d}
    WHERE bouwjaar > date_part('YEAR', ahn_file_date);
    """).format(bag3d=name_q, 
                viewname=viewname)
    query_vc = sql.SQL("""
    COMMENT ON VIEW bagactueel.{viewname} IS \
    'The BAG footprints where the building was built before the AHN3 was created';
    """).format(viewname=viewname)

    viewname = sql.Identifier(config["quality"]["views"]["missing_ground"])
    query_g = sql.SQL("""
    CREATE OR REPLACE VIEW bagactueel.{viewname} AS
    SELECT *
    FROM bagactueel.{bag3d}
    WHERE
    "ground-0.00" IS NULL OR 
    "ground-0.10" IS NULL OR 
    "ground-0.20" IS NULL OR 
    "ground-0.30" IS NULL OR 
    "ground-0.40" IS NULL OR 
    "ground-0.50" IS NULL;
    COMMENT ON VIEW bagactueel.{viewname} IS 'Buildings where any of the ground-heights is missing';
    """).format(bag3d=sql.Identifier(config["quality"]["views"]["valid"]), 
                viewname=viewname)

    viewname = sql.Identifier(config["quality"]["views"]["missing_roof"])
    query_r = sql.SQL("""
    CREATE OR REPLACE VIEW bagactueel.{viewname} AS
    SELECT *
    FROM bagactueel.{bag3d}
    WHERE
    "roof-0.00" IS NULL OR
    "roof-0.10" IS NULL OR
    "roof-0.25" IS NULL OR
    "roof-0.50" IS NULL OR
    "roof-0.75" IS NULL OR
    "roof-0.90" IS NULL OR
    "roof-0.95" IS NULL OR
    "roof-0.99" IS NULL;
    COMMENT ON VIEW bagactueel.{viewname} IS 'Buildings where any of the roof heights is missing';
    """).format(bag3d=sql.Identifier(config["quality"]["views"]["valid"]), 
                viewname=viewname)
    
    tbl_schema = config["tile_index"]['elevation']['schema']
    tbl_name = config["tile_index"]['elevation']['table']
    tbl_tile = config["tile_index"]['elevation']['fields']['unit_name']
    border_table = config["tile_index"]['elevation']['border_table']
    tr = border.get_non_border_tiles(conn, tbl_schema, tbl_name, border_table,
                                     tbl_tile)
    t_rest = [t[0] for t in tr]
    viewname = sql.Identifier(config["quality"]["views"]["sample"])
    size = sql.Literal(float(config["quality"]["sample_size"]))
    query_s = sql.SQL("""
    CREATE OR REPLACE VIEW bagactueel.{viewname} AS
    SELECT *
    FROM bagactueel.{bag3d}
    TABLESAMPLE BERNOULLI ({size})
    WHERE tile_id = ANY({non_border_tiles});
    COMMENT ON VIEW bagactueel.{viewname} IS 'Random sample of the 3D BAG, using Bernoulli sampling method. Border tiles are excluded.';
    """).format(bag3d=sql.Identifier(config["quality"]["views"]["valid"]), 
                viewname=viewname,
                size=size,
                non_border_tiles=sql.Literal(t_rest))
    
    try:
        logger.debug(conn.print_query(query_valid))
        conn.sendQuery(query_valid)
    except BaseException as e:
        logger.exception(e)
        raise
    try:
        logger.debug(conn.print_query(query_v))
        conn.sendQuery(query_v)
        logger.debug(conn.print_query(query_vc))
        conn.sendQuery(query_vc)
        logger.debug(conn.print_query(query_g))
        conn.sendQuery(query_g)
        logger.debug(conn.print_query(query_r))
        conn.sendQuery(query_r)
        logger.debug(conn.print_query(query_s))
        conn.sendQuery(query_s)
    except BaseException as e:
        logger.exception(e)
        raise
    
    return config

def create_quality_table(conn):
    """Create a table to store the quality statistics"""
    query = sql.SQL("""
    CREATE TABLE IF NOT EXISTS public.bag3d_quality (
    id SERIAL PRIMARY KEY,
    timestamp timestamptz, 
    total_cnt int,
    valid_height_pct float4,
    invalid_height_pct float4,
    ground_missing_pct float4,
    roof_missing_pct float4,
    building_cnt json
    );
    """)
    try:
        logger.debug(conn.print_query(query))
        conn.sendQuery(query)
    except BaseException as e:
        logger.exception(e)
        raise

def get_counts(conn, config):
    """Various counts on the 3D BAG
    
    * Total number of buildings, 
    * Nr. of buildings with missing ground height,
    * Nr. of buildings with missing roof height,
    * The previous two as percent
    
    Parameters
    ----------
    conn : :py:class:`bag3d.config.db.db`
        Open connection
    config: dict
        batch3dfier YAML config as returned by :meth:`bag3d.config.args.parse_config`
    
    Returns
    -------
    dict
        With the field names as keys
    """
    schema = sql.Identifier(config['input_polygons']['footprints']['schema'])
    query = sql.SQL("""
    WITH total AS (
        SELECT
            COUNT(gid) total_cnt
        FROM
            {schema}.{bag3d}
    ),
    ground AS (
        SELECT
            COUNT(gid) ground_missing_cnt
        FROM
            {schema}.{bag3d}
        WHERE nr_ground_pts = 0
    ),
    roof AS (
        SELECT
            COUNT(gid) roof_missing_cnt
        FROM
            {schema}.{bag3d}
        WHERE nr_roof_pts = 0
    ),
    invalid AS (
        SELECT
            COUNT (gid) invalid_height_cnt
        FROM
            {schema}.{bag3d}
        WHERE bouwjaar > ahn_file_date
    )
    SELECT
        current_timestamp AS timestamp,
        t.total_cnt,
        (t.total_cnt::float4 - i.invalid_height_cnt::float4) / t.total_cnt::float4 * 100 AS valid_height_pct,
        i.invalid_height_cnt::float4 / t.total_cnt::float4 * 100 AS invalid_height_pct,
        (
            g.ground_missing_cnt::FLOAT4 / t.total_cnt::FLOAT4
        )* 100 AS ground_missing_pct,
        (
            r.roof_missing_cnt::FLOAT4 / t.total_cnt::FLOAT4
        )* 100 AS roof_missing_pct
    FROM
        total t,
        ground g,
        roof r,
        invalid i;
    """).format(bag3d=sql.Identifier(config["output"]["bag3d_table"]),
                schema=schema)
    try:
        logger.debug(conn.print_query(query))
        res = conn.get_dict(query)
        logger.debug(res)
        return res
    except BaseException as e:
        logger.exception(e)
        raise

def buildings_per_tile(conn, config):
    """Count the number of buildings in the BAG and the 3D BAG per tile"""
    schema = sql.Identifier(config['input_polygons']['footprints']['schema'])
    table_bag_q = sql.Identifier(config['input_polygons']['footprints']['table'])
    schema_idx_q = sql.Identifier(config['tile_index']['polygons']['schema'])
    table_idx_q = sql.Identifier(config['tile_index']['polygons']['table'])
    field_idx_unit_q = sql.Identifier(config['tile_index']['polygons']['fields']['unit_name'])
    field_idx_geom_q = sql.Identifier(config['tile_index']['polygons']['fields']['geometry'])

    query = sql.SQL("""
    WITH bag_tiles AS (
    SELECT
        {table_bag}.gid,
        {table_idx}.{field_idx}
    FROM
        {schema}.{table_bag}
    JOIN {schema}.pand_centroid ON
        {table_bag}.gid = pand_centroid.gid,
        {schema_idx}.{table_idx}
    WHERE
        st_containsproperly({table_idx}.{field_idx_geom}, pand_centroid.geom)
        OR st_contains({table_idx}.geom_border, pand_centroid.geom)
    ),
    bag_tiles_cnt AS (
        SELECT {field_idx} AS tile_id, count(*) AS bag_cnt
        FROM bag_tiles
        GROUP BY {field_idx}
    ),
    bag3d_tiles_cnt AS (
        SELECT tile_id, count(*) AS bag3d_cnt
        FROM {schema}.{bag3d}
        GROUP BY tile_id
    ),
    counts AS (
        SELECT a.tile_id, bag_cnt, bag3d_cnt
        FROM bag_tiles_cnt a
        LEFT JOIN bag3d_tiles_cnt b ON a.tile_id = b.tile_id
    )
    SELECT array_to_json(array_agg(counts)) AS building_cnt
    FROM counts;
    """).format(bag3d=sql.Identifier(config["output"]["bag3d_table"]),
                schema=schema,
                table_bag=table_bag_q,
                schema_idx=schema_idx_q,
                table_idx=table_idx_q,
                field_idx=field_idx_unit_q,
                field_idx_geom=field_idx_geom_q
                )
    try:
        logger.debug(conn.print_query(query))
        res = conn.getQuery(query)
        logger.debug(res)
        return res
    except BaseException as e:
        logger.exception(e)
        raise

def update_quality_table(conn, counts, buildings_per_tile):
    """Inserts the quality metrics into the quality table"""
    try:
        with conn.conn:
            with conn.conn.cursor() as cur:
                cur.execute("""
                INSERT INTO public.bag3d_quality (
                    timestamp, 
                    total_cnt,
                    valid_height_pct,
                    invalid_height_pct,
                    ground_missing_pct,
                    roof_missing_pct,
                    building_cnt
                ) VALUES (%(time)s, %(total)s, %(valid)s, %(invalid)s, %(ground)s, %(roof)s, %(building)s);
                """, {'time': counts[0]['timestamp'],
                    'total': counts[0]['total_cnt'],
                    'valid': counts[0]['valid_height_pct'],
                    'invalid': counts[0]['invalid_height_pct'],
                    'ground': counts[0]['ground_missing_pct'],
                    'roof': counts[0]['roof_missing_pct'],
                    'building': Json(buildings_per_tile[0][0])
                      }
                            )
    except BaseException as e:
        logger.exception(e)
        raise

def get_sample(conn, config):
    """Get a random sample of buildings from the 3D BAG
    
    Sample size is defined in create_quality_views()
    
    Parameters
    ----------
    conn : :py:class:`bag3d.config.db.db`
        Open connection
    cfg: dict
        batch3dfier YAML config as returned by :meth:`bag3d.config.args.parse_config`
    
    Returns
    -------
    dict
    """
    viewname = sql.Identifier(config["quality"]["views"]["sample"])
    geom = sql.Identifier(config["input_polygons"]["footprints"]["fields"]["geometry"])
    query = sql.SQL("""
    SELECT 
    gid,
    ST_AsEWKB({geom}) geom,
    "roof-0.00" "percentile_0.00",
    "roof-0.10" "percentile_0.10",
    "roof-0.25" "percentile_0.25",
    "roof-0.50" "percentile_0.50",
    "roof-0.75" "percentile_0.75",
    "roof-0.90" "percentile_0.90",
    "roof-0.95" "percentile_0.95",
    "roof-0.99" "percentile_0.99",
    tile_id,
    ahn_version
    FROM bagactueel.{viewname}
    ORDER BY tile_id;
    """).format(geom=geom,
                viewname=viewname)
    logger.debug(conn.print_query(query))
    return conn.get_dict(query)


def compute_stats(sample, file_idx, stats):
    """Compute statistics from a reference data set for comparison with 
    3dfier's output
    """
    logger.info("Computing %s from reference data", stats)
    tiles = set([fp["tile_id"] for fp in sample])
    out = []
    logger.debug("%s tiles selected" % len(tiles))
    for tile in tiles:
        if tile in file_idx:
            rast = file_idx[tile]
            fprints_in_tile = []
            for i,fp in enumerate(sample):
                if isinstance(fp, list):
                    logger.error("oh oh, unexpected list")
                    logger.debug(fp[0])
                else:
                    if fp["tile_id"] == tile:
                        fprints_in_tile.append(fp)
                        del sample[i]
            polys = [bytes(fp['geom']) for fp in fprints_in_tile]
            ref_heights = zonal_stats(polys, rast, stats=stats)
            for i,fp in enumerate(fprints_in_tile):
                fp['reference'] = ref_heights[i]
                out.append(fp)
        else:
            logger.debug("%s not in raster index", tile)
            pass
    return out


def export_stats(sample, fout):
    stats = {fp['gid']:fp for fp in sample}
    with open(fout, 'w') as f:
        json.dump(stats, f)


def compute_diffs(sample, stats):
    """
      
    Parameters
    ----------
    sample : list of dict
        Sample with reference heights
      
    Returns
    -------
    dict
        {percentile : Numpy Array of 'computed-height - reference-height' differences}
    """
    diffs = []
    fields = ['gid','ahn_version','tile_id'] + stats
    logger.debug(fields)
    for fp in sample:
        d = {}
        for col in fields:
            if 'percentile' in col:
                if fp[col] and fp["reference"][col]:
                    d[col] = fp[col] - fp["reference"][col]
                else:
                    d[col] = None
            else:
                d[col] = fp[col]
        diffs.append(d)
    return (diffs,fields)


def rmse(a):
    """Compute Root Mean Square Error from a Numpy Array of height - reference 
    differences
    """
    return sqrt(sum([d**2 for d in a]) / len(a))

def compute_rmse(diffs, stats):
    """Compute the RMSE across the whole sample"""
    res = {}
    for pctile in stats:
        logger.debug("Computing %s", pctile)
        r = []
        for fp in diffs:
            r.append(fp[pctile])
        a = np.array(r, dtype='float32')
        logger.debug(a)
        res[pctile] = round(rmse(a[~np.isnan(a)]),2)
    return res
