# -*- coding: utf-8 -*-

"""Quality control for the 3D BAG"""

import logging
import json
from math import sqrt

from psycopg2 import sql
import numpy as np
from rasterstats import zonal_stats

logger = logging.getLogger("quality")


def create_quality_views(conn, config):
    """Create the views that are used for quality control"""
    
    name = config["output"]["bag3d_table"]
    name_q = sql.Identifier(name)
    
    viewname = sql.Identifier(name + "_invalid_height")
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

    viewname = sql.Identifier(name + "_missing_ground")
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
    """).format(bag3d=name_q, viewname=viewname)

    viewname = sql.Identifier(name + "_missing_roof")
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
    """).format(bag3d=name_q, viewname=viewname)
    
    viewname = sql.Identifier(name + "_sample")
    query_s = sql.SQL("""
    CREATE OR REPLACE VIEW bagactueel.{viewname} AS
    SELECT *
    FROM bagactueel.{bag3d}
    TABLESAMPLE BERNOULLI (1);
    COMMENT ON VIEW bagactueel.{viewname} IS 'Random sample (1%) of the 3D BAG, using Bernoulli sampling method';
    """).format(bag3d=name_q, viewname=viewname)
    
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


def get_counts(conn, name):
    """Various counts on the 3D BAG
    
    * Total number of buildings, 
    * Nr. of buildings with missing ground height,
    * Nr. of buildings with missing roof height,
    * The previous two as percent
    
    Returns
    -------
    dict
        With the field names as keys
    """
    view_h = sql.Identifier(name + "_invalid_height")
    view_g = sql.Identifier(name + "_missing_ground")
    view_r = sql.Identifier(name + "_missing_roof")
    query = sql.SQL("""
    WITH total AS (
        SELECT
            COUNT(gid) total_cnt
        FROM
            bagactueel.{bag3d}
    ),
    ground AS (
        SELECT
            COUNT(gid) ground_missing_cnt
        FROM
            bagactueel.{view_g}
    ),
    roof AS (
        SELECT
            COUNT(gid) roof_missing_cnt
        FROM
            bagactueel.{view_r}
    ),
    invalid AS (
        SELECT
            COUNT (gid) invalid_height_cnt
        FROM
            bagactueel.{view_h}
    ) SELECT
        g.ground_missing_cnt,
        r.roof_missing_cnt,
        i.invalid_height_cnt,
        t.total_cnt,
        (
            g.ground_missing_cnt::FLOAT4 / t.total_cnt::FLOAT4
        )* 100 AS ground_missing_pct,
        (
            r.roof_missing_cnt::FLOAT4 / t.total_cnt::FLOAT4
        )* 100 AS roof_missing_pct,
        (
            i.invalid_height_cnt::FLOAT4 / t.total_cnt::FLOAT4
        )* 100 AS invalid_height_pct
    FROM
        total t,
        ground g,
        roof r,
        invalid i;
    """).format(bag3d=sql.Identifier(name),
                view_g=view_g, view_r=view_r, view_h=view_h)
    try:
        logger.debug(conn.print_query(query))
        res = conn.get_dict(query)
        return res[0]
    except BaseException as e:
        logger.exception(e)
        raise


def get_sample(conn, config):
    """Get a random sample of buildings from the 3D BAG
    
    Sample size is defined in create_quality_views()
    """
    viewname = sql.Identifier(config["output"]["bag3d_table"] + "_sample")
    geom = sql.Identifier(config["input_polygons"]["footprints"]["fields"]["geometry"])
    query = sql.SQL("""
    SELECT 
    identificatie,
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


def compute_stats(sample, file_idx):
    """Compute statistics from a reference data set for comparison with 
    3dfier's output
    """
    stats=['percentile_0.00', 'percentile_0.10', 'percentile_0.25',
           'percentile_0.50', 'percentile_0.75', 'percentile_0.90',
           'percentile_0.95', 'percentile_0.99']
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
            pass
    return out


def export_stats(sample, fout):
    stats = {fp['identificatie']:fp for fp in sample}
    with open(fout, 'w') as f:
        json.dump(stats, f)

# 
# def compute_diffs(sample):
#     """
#      
#     Parameters
#     ----------
#     sample : list of dict
#         Sample with reference heights
#      
#     Returns
#     -------
#     dict
#         {percentile : Numpy Array of 'reference-height - computed-height' differences}
#     """
#     diffs = []
#     for fp in sample:
#         d = {
#             'identificatie': fp["identificatie"],
#             'ahn_version': fp["ahn_version"],
#             'tile_id': fp["tile_id"],
#             "percentile_0.00": fp["percentile_0.00"] - fp["reference"]["percentile_0.00"],
#             "percentile_0.10": fp["percentile_0.10"] - fp["reference"]["percentile_0.10"],
#             "percentile_0.25": fp["percentile_0.25"] - fp["reference"]["percentile_0.25"],
#             "percentile_0.50": fp["percentile_0.50"] - fp["reference"]["percentile_0.50"],
#             "percentile_0.75": fp["percentile_0.75"] - fp["reference"]["percentile_0.75"],
#             "percentile_0.90": fp["percentile_0.90"] - fp["reference"]["percentile_0.90"],
#             "percentile_0.95": fp["percentile_0.95"] - fp["reference"]["percentile_0.95"],
#             "percentile_0.99": fp["percentile_0.99"] - fp["reference"]["percentile_0.99"]
#              }
#         for i, row in enumerate(heights):
#             if ref_heights[i][k]:
#                 diffs[k].append(row[k]- ref_heights[i][k])
#             else:
#                 diffs[k].append(None)
#         diffs[k] = np.array(diffs[k], dtype='float32')
#     return diffs
#  
# diffs = compute_diffs(stats, heights=res, ref_heights=ref_heights)

def rmse(a):
    """Compute Root Mean Square Error from a Numpy Array of height - reference 
    differences
    """
    return sqrt(sum([d**2 for d in a]) / len(a))

def compute_rmse(diffs):
    res = {}
    for k in diffs:
        res[k] = rmse(diffs[k])
    return res
