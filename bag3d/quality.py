# -*- coding: utf-8 -*-

"""Quality control for the 3D BAG"""

import logging

from psycopg2 import sql

logger = logging.getLogger("quality")


def create_quality_views(conn, config):
    """Create the views that are used for quality control"""
    
    name = config["output"]["bag3d_table"]
    name_q = sql.Identifier(name)
    
    viewname = sql.Identifier(name + "_valid_height")
    query_v = sql.SQL("""
    CREATE OR REPLACE VIEW bagactueel.{viewname} AS
    SELECT *
    FROM bagactueel.{bag3d}
    WHERE bouwjaar <= date_part('YEAR', ahn_file_date);
    """).format(bag3d=name_q, 
                viewname=viewname)
    
    query_vc = sql.SQL("""
    COMMENT ON VIEW bagactueel.{viewname} IS \
    'The BAG footprints where the building was built before the AHN3 was created';
    """).format(viewname=viewname)

    query_g = sql.SQL("""
    CREATE OR REPLACE VIEW bagactueel.missing_ground AS
    SELECT *
    FROM bagactueel.{bag3d}
    WHERE
    "ground-0.00" IS NULL OR 
    "ground-0.10" IS NULL OR 
    "ground-0.20" IS NULL OR 
    "ground-0.30" IS NULL OR 
    "ground-0.40" IS NULL OR 
    "ground-0.50" IS NULL;
    COMMENT ON VIEW bagactueel.missing_ground IS 'Buildings where any of the ground-heights is missing';
    """).format(bag3d=name_q)

    query_r = sql.SQL("""
    CREATE OR REPLACE VIEW bagactueel.missing_roof AS
    SELECT *
    FROM bagactueel.bag3d
    WHERE
    "roof-0.00" IS NULL OR
    "roof-0.10" IS NULL OR
    "roof-0.25" IS NULL OR
    "roof-0.50" IS NULL OR
    "roof-0.75" IS NULL OR
    "roof-0.90" IS NULL OR
    "roof-0.95" IS NULL OR
    "roof-0.99" IS NULL;
    COMMENT ON VIEW bagactueel.missing_roof IS 'Buildings where any of the roof heights is missing';
    """).format(bag3d=name_q)
    
    try:
        logger.debug(conn.print_query(query_v))
        conn.sendQuery(query_v)
        logger.debug(conn.print_query(query_vc))
        conn.sendQuery(query_vc)
        logger.debug(conn.print_query(query_g))
        conn.sendQuery(query_g)
        logger.debug(conn.print_query(query_r))
        conn.sendQuery(query_r)
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
    
    query = sql.SQL("""
    WITH total AS (
    SELECT
        COUNT( gid ) total_cnt
    FROM
        bagactueel.{bag3d}
    ),
    ground AS (
        SELECT
            COUNT( gid ) ground_missing_cnt
        FROM
            bagactueel.missing_ground
    ),
    roof AS (
        SELECT
            COUNT( gid ) roof_missing_cnt
        FROM
            bagactueel.missing_height
    ) SELECT
        g.ground_missing_cnt,
        r.roof_missing_cnt,
        t.total_cnt,
        (
            g.ground_missing_cnt::FLOAT4 / t.total_cnt::FLOAT4
        )* 100 AS ground_missing_pct,
        (
            r.roof_missing_cnt::FLOAT4 / t.total_cnt::FLOAT4
        )* 100 AS roof_missing_pct
    FROM
        total t,
        ground g,
        roof r;
    """).format(bag3d=sql.Identifier(name))
    try:
        logger.debug(conn.print_query(query))
        res = conn.get_dict(query)
        return res[0]
    except BaseException as e:
        logger.exception(e)
        raise