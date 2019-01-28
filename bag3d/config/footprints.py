# -*- coding: utf-8 -*-

"""Batch create views for the 2D(BAG) tiles.

The module helps you to create tiles in a BAG (https://www.kadaster.nl/wat-is-de-bag)
database. These tiles are then used by batch3dfier.
"""
import logging
from psycopg2 import sql

logger = logging.getLogger(__name__)


def update_tile_index(db, table_index, fields_index):
    """Update the tile index to include the lower/left boundary of each polygon.

    The function is mainly relevant for the tile index of the footprints.
    The tile edges are then used for checking centroid containment in a tile polygon.

    Parameters
    ----------
    db : :py:class:`bag3d.config.db.db`
    table_index : list of str
        (schema, table) that contains the tile index polygons.
    fields_index: list of str
        [ID, geometry, unit] field names of the ID, geometry, tile unit name fields in table_index.

    Returns
    -------
    nothing
        nothing
    """
    schema = table_index[0]
    table = table_index[1]
    id_col = fields_index[0]
    geom_col = fields_index[1]

    schema_q = sql.Identifier(schema)
    table_q = sql.Identifier(table)
    geom_col_q = sql.Identifier(geom_col)
    id_col_q = sql.Identifier(id_col)
    
    query = sql.SQL("""
    ALTER TABLE {}.{}
    ADD COLUMN IF NOT EXISTS geom_border geometry;
    """).format(schema_q, table_q)
    db.sendQuery(query)
    logger.debug(db.print_query(query))

    query = sql.SQL("""
    UPDATE
        {schema}.{table}
    SET
        geom_border = b.geom::geometry(linestring,28992)
    FROM
        (
            SELECT
                {id_col},
                st_setSRID(
                    st_makeline(
                        ARRAY[st_makepoint(
                            st_xmax({geom_col}),
                            st_ymin({geom_col})
                        ),
                        st_makepoint(
                            st_xmin({geom_col}),
                            st_ymin({geom_col})
                        ),
                        st_makepoint(
                            st_xmin({geom_col}),
                            st_ymax({geom_col})
                        ) ]
                    ),
                    28992
                ) AS geom
            FROM
                {schema}.{table}
        ) b
    WHERE
        {schema}.{table}.{id_col} = b.{id_col};
    """).format(schema=schema_q,
            table=table_q,
            geom_col=geom_col_q,
            id_col=id_col_q)
    logger.debug(db.print_query(query))
    db.sendQuery(query)

    sql_query = sql.SQL("""
    CREATE INDEX IF NOT EXISTS {idx_name} ON {schema}.{table} USING gist (geom_border);
    SELECT populate_geometry_columns({name}::regclass);
    """).format(idx_name=sql.Identifier(table + "_" + geom_col + "_border_idx"),
            schema=schema_q,
            table=table_q,
            name=sql.Literal(schema + '.' + table))
    logger.debug(db.print_query(sql_query))
    db.sendQuery(sql_query)
    db.vacuum(schema, table)


def create_centroids(db, table_centroid, table_footprint, fields_footprint):
    """Creates a table of footprint centroids.

    The table_centroid is then used by bagtiler().

    Parameters
    ----------
    db : :py:class:`bag3d.config.db.db`
    table_centroid : list of str
        [schema, table] for the new relation that contains the footprint centroids.
    table_footprint : list of str
        [schema, table] of the footprints (e.g. building footprints) that will be extruded.
    fields_footprint : list of str
        [ID, geometry] field names of the ID geometry fields in table_footprint.

    Returns
    -------
    nothing
        nothing

    """
    schema_ctr = table_centroid[0]
    table_ctr = table_centroid[1]
    schema_poly = table_footprint[0]
    table_poly = table_footprint[1]
    id_col = fields_footprint[0]
    geom_col = fields_footprint[1]

    schema_ctr_q = sql.Identifier(schema_ctr)
    table_ctr_q = sql.Identifier(table_ctr)
    schema_poly_q = sql.Identifier(schema_poly)
    table_poly_q = sql.Identifier(table_poly)
    geom_col_q = sql.Identifier(geom_col)
    id_col_q = sql.Identifier(id_col)

    sql_query = sql.SQL("""
    CREATE TABLE IF NOT EXISTS {schema_ctr}.{table_ctr} AS
        SELECT {id_col}, st_centroid({geom_col})::geometry(point, 28992) AS geom
        FROM {schema_poly}.{table_poly};
    
    SELECT populate_geometry_columns({sch_tbl}::regclass);
    
    CREATE
        INDEX IF NOT EXISTS {tbl_idx} ON
        {schema_ctr}.{table_ctr}
            USING gist(geom);
    """).format(schema_ctr=schema_ctr_q,
            table_ctr=table_ctr_q,
            id_col=id_col_q,
            geom_col=geom_col_q,
            schema_poly=schema_poly_q,
            table_poly=table_poly_q,
            sch_tbl=sql.Literal(schema_ctr + '.' + table_ctr),
            tbl_idx=sql.Identifier(table_ctr + '_geom_idx')
            )
    logger.debug(db.print_query(sql_query))
    db.sendQuery(sql_query)
    db.vacuum(schema_ctr, table_ctr)


def create_views(db, schema_tiles, table_index, fields_index, table_centroid,
                 fields_centroid, table_footprint, fields_footprint,
                 prefix_tiles='t_'):
    """Creates PostgreSQL Views for the footprint tiles.

    Parameters
    ----------
    db : :py:class:`bag3d.config.db.db`
    schema_tiles : str
        Name of the schema where to create the footprint tiles.
    table_index : list of str
        [schema, table] of the tile index.
    fields_index : list of str
        [ID, geometry, unit]
        ID: Name of the ID field.
        geometry: Name of the geometry field.
        unit: Name of the field in table_index that contains the index unit names.
        These values are used for the tile names in schema_tiles.
    table_centroid : list of str
        [schema, table] of the footprint centroids.
    fields_centroid : list of str
        [ID, geometry]
        Name of the ID field in table_centroid that can be joined on table_footprint.
        There must be an identical value in fields_footprint.
        Name of the geometry field.
    table_footprint : list of str
        [schema, table] of the footprints (e.g. building footprints) that will be extruded.
    fields_footprint : list of str
        [ID, geometry, ...]
        Names of the fields that should be selected into the View. Must contain
        at least an ID and a geometry field, where ID is the field that can be joined on
        table_centroid.
    prefix_tiles : str or None
        Prefix to prepend to the view names. If None, the views are named as
        the values in fields_index.

    Returns
    -------
    nothing
        nothing

    """
    schema_tiles_q = sql.Identifier(schema_tiles)

    schema_idx_q = sql.Identifier(table_index[0])
    table_idx_q = sql.Identifier(table_index[1])
    field_idx_unit_q = sql.Identifier(fields_index[2])
    field_idx_geom_q = sql.Identifier(fields_index[1])

    schema_ctr_q = sql.Identifier(table_centroid[0])
    table_ctr_q = sql.Identifier(table_centroid[1])
    field_ctr_id = fields_centroid[0]
    field_ctr_id_q = sql.Identifier(field_ctr_id)
    field_ctr_geom_q = sql.Identifier(fields_centroid[1])

    table_poly = table_footprint[1]
    schema_poly_q = sql.Identifier(table_footprint[0])
    table_poly_q = sql.Identifier(table_poly)

    assert isinstance(fields_footprint, list)
    assert len(fields_footprint) > 1,\
        "You must provide at least two fields (e.g. id, geometry)"
    assert field_ctr_id in fields_footprint,\
        "There must be a join field for table_centroid and table_footprint."
    # prepare SELECT FROM table_footprint
    s = []
    for f in fields_footprint:
        s.append(sql.SQL('.').join(
            [sql.Identifier(table_poly), sql.Identifier(f)]))
    sql_fields_footprint = sql.SQL(', ').join(s)

    field_poly_id_q = sql.Identifier(fields_footprint[0])
#     print(sql_fields_footprint.as_string(dbs.conn))

    # Create schema to store the tiles
    query = sql.SQL("CREATE SCHEMA IF NOT EXISTS {};").format(schema_tiles_q)
    logger.debug(db.print_query(query))
    db.sendQuery(query)

    # Get footprint index unit names
    tiles = db.getQuery(sql.SQL("SELECT {} FROM {}.{};").format(
        field_idx_unit_q,schema_idx_q,table_idx_q))
    tiles = [str(i[0]) for i in tiles]

    if not prefix_tiles:
        prefix_tiles = ""
    assert isinstance(prefix_tiles, str)
    # Create a BAG tile with equivalent area of an AHN tile
    queries = sql.Composed('')
    for tile in tiles:
        # !!! the 't_' prefix is hard-coded in config.call3dfier() !!!
        n = prefix_tiles + str(tile)
        view = sql.Identifier(n)

        tile = sql.Literal(tile)
        query = sql.SQL("""
    CREATE OR REPLACE VIEW {schema_tiles}.{view} AS
    SELECT
        {fields_poly},
        {table_idx}.{field_idx}
    FROM
        {schema_poly}.{table_poly}
    INNER JOIN {schema_ctr}.{table_ctr} ON
        {table_poly}.{field_poly_id} = {table_ctr}.{field_ctr_id},
        {schema_idx}.{table_idx}
    WHERE
        {table_idx}.{field_idx} = {tile}
        AND(
            st_containsproperly(
                {table_idx}.{field_idx_geom},
                {table_ctr}.{field_ctr_geom}
            )
            OR st_contains(
                {table_idx}.geom_border,
                {table_ctr}.{field_ctr_geom}
            )
    );""").format(schema_tiles=schema_tiles_q,
              view=view,
              fields_poly=sql_fields_footprint,
              schema_poly=schema_poly_q,
              table_poly=table_poly_q,
              schema_ctr=schema_ctr_q,
              table_ctr=table_ctr_q,
              field_poly_id=field_poly_id_q,
              field_ctr_id=field_ctr_id_q,
              schema_idx=schema_idx_q,
              table_idx=table_idx_q,
              field_idx=field_idx_unit_q,
              tile=tile,
              field_idx_geom=field_idx_geom_q,
              field_ctr_geom=field_ctr_geom_q
              )
        queries += query
    logger.debug(db.print_query(queries))
    db.sendQuery(queries)

    logger.debug("%s Views created in schema '%s'." % (len(tiles), schema_tiles))

#     except:
#         return("Cannot create Views in schema '%s'" % schema_tiles)


def partition(db, schema_tiles, table_index, fields_index, table_footprint,
              fields_footprint, prefix_tiles):
    """Partitions geometries in a 2D footprint table into tiles.

    Adds a geometry column to table_index.
    Creates a new table <table_footprint>_centroid in the schema of table_footprint.
    Creates a View for each tile in table_index, generating names from fields_index[2].

    Parameters
    ----------
    db : :py:class:`bag3d.config.db.db`
    schema_tiles : str
        Name of the schema where to create the tiles.
    table_index : list of str
        [schema, table] of the tile index.
    fields_index : list of str
        [ID, geometry, unit]
        ID: Name of the ID field.
        geometry: Name of the geometry field.
        unit: Name of the field in table_index that contains the index unit names.
        These values are used for the tile names in schema_tiles.
    table_footprint : list of str
        [schema, table] of the footprints (e.g. building footprints) that will be extruded.
    fields_footprint : list of str
        [ID, geometry, ...]
        Names of the fields that should be selected into the View. Must contain
        at least an ID and a geometry field, where ID is the field that can be joined on
        table_centroid.
    prefix_tiles : str, None
        Prefix to prepend to the view names. If None, the views are named as
        the values in fields_index[2].

    Returns
    -------
    nothing
        nothing

    """

    tbl_ctr = table_footprint[1] + "_centroid"
    table_centroid = [table_footprint[0], tbl_ctr]
    fields_centroid = [fields_footprint[0], 'geom']

    update_tile_index(db, table_index, fields_index)

    create_centroids(db, table_centroid, table_footprint, fields_footprint)

    create_views(db, schema_tiles, table_index, fields_index, table_centroid,
                 fields_centroid, table_footprint, fields_footprint,
                 prefix_tiles)
