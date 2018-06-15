# -*- coding: utf-8 -*-

"""Configure batch3dfier with the input data."""

import os.path
import re
from itertools import chain
from pprint import pformat

from shapely.geometry import shape
from shapely import geos
from psycopg2 import sql
import fiona
import logging

from bag3d.update import bag

logger = logging.getLogger('config.batch3dfier')


def call_3dfier(db, tile, schema_tiles,
                table_index_pc, fields_index_pc,
                table_index_footprint, fields_index_footprint, uniqueid,
                extent_ewkb, clip_prefix, prefix_tile_footprint,
                yml_dir, tile_out, output_format, output_dir,
                path_3dfier, thread,
                pc_file_index,
                doexec=True):
    """Call 3dfier with the YAML config created by yamlr().

    Note
    ----
    For the rest of the parameters see batch3dfier_config.yml.

    Parameters
    ----------
    db : :py:class:`bag3d.config.db.db`
        Open connection
    tile : str
        Name of of the 2D tile.
    schema_tiles : str
        Schema of the footprint tiles.
    thread : str
        Name/ID of the active thread.
    extent_ewkb : str
        EWKB representation of 'extent' in batch3dfier_config.yml.
    clip_prefix : str
        Prefix for naming the clipped/united views. This value shouldn't be a substring of the pointcloud file names.
    prefix_tile_footprint : str or None
        Prefix prepended to the footprint tile view names. If None, the views are named as
        the values in fields_index_fooptrint['unit_name'].

    Returns
    -------
    list
        The tiles that are skipped because no corresponding pointcloud file
        was found in 'dataset_dir' (YAML)

    """
    pc_tiles = find_pc_tiles(db, table_index_pc, fields_index_pc,
                             table_index_footprint, fields_index_footprint,
                             extent_ewkb, tile_footprint=tile,
                             prefix_tile_footprint=prefix_tile_footprint)
    logger.debug("call_3dfier:pc_tiles: %s", pc_tiles)
#     pc_path = find_pc_files(pc_tiles, pc_dir, pc_dataset_name, pc_tile_case)
#     pc_path = [t for tile in pc_tiles for t in pc_file_index[tile]]
    tileset = {i for i in pc_tiles}
    p = [pc_file_index[tile] for tile in pc_file_index.keys() & tileset]
    pc_path = list(chain.from_iterable(p))
    logger.debug("call_3dfier:pc_path: %s", pc_path)
    # prepare output file name
    if not tile_out:
        tile_out = tile.replace(clip_prefix, '', 1)

    # Call 3dfier ------------------------------------------------------------

    if pc_path:
        # Needs a YAML per thread so one doesn't overwrite it while the other
        # uses it
        yml_name = thread + "_config.yml"
        yml_path = os.path.join(yml_dir, yml_name)
        config = yamlr(dbname=db.dbname, host=db.host, user=db.user,
                       pw=db.password, schema_tiles=schema_tiles,
                       bag_tile=tile, pc_path=pc_path,
                       output_format=output_format, uniqueid=uniqueid)
        # Write temporary config file
        try:
            with open(yml_path, "w") as text_file:
                text_file.write(config)
        except BaseException as e:
            logger.exception("Error: cannot write _config.yml")
        # Prep output file name
        if "obj" in output_format.lower():
            o = tile_out + ".obj"
            output_path = os.path.join(output_dir, o)
        elif "csv" in output_format.lower():
            o = tile_out + ".csv"
            output_path = os.path.join(output_dir, o)
        else:
            output_path = os.path.join(output_dir, tile_out)
        # Run 3dfier
        command = [path_3dfier, yml_path, "-o", output_path]
        try:
            bag.run_subprocess(command, shell=True, doexec=doexec)
        except BaseException as e:
            logger.exception("\nCannot run 3dfier on tile " + tile)
            tile_skipped = tile
    else:
        logger.debug(
            "\nPointcloud file(s) " +
            str(pc_tiles) +
            " not available. Skipping tile.\n")
        tile_skipped = tile
        return({'tile_skipped': tile_skipped,
                'out_path': None})
    return({'tile_skipped': None,
            'out_path': output_path})


def yamlr(dbname, host, user, pw, schema_tiles,
          bag_tile, pc_path, output_format, uniqueid):
    """Parse the YAML config file for 3dfier.

    Parameters
    ----------
    See batch3dfier_config.yml.


    Returns
    -------
    string
        the YAML config file for 3dfier

    """

    pc_dataset = ""
    if len(pc_path) > 1:
        for p in pc_path:
            pc_dataset += "- " + p + "\n" + "      "
    else:
        pc_dataset += "- " + pc_path[0]

    # !!! Do not correct the indentation of the config template, otherwise it
    # results in 'YAML::TypedBadConversion<std::__cxx11::basic_string<char, std::char_traits<char>, std::allocator<char> > >'
    # because every line is indented as here
    config = """
input_polygons:
  - datasets:
      - "PG:dbname={dbname} host={host} user={user} password={pw} schemas={schema_tiles} tables={bag_tile}"
    uniqueid: {uniqueid}
    lifting: Building

lifting_options:
  Building:
    height_roof: percentile-90
    height_floor: percentile-10
    lod: 1

input_elevation:
  - datasets:
      {pc_path}
    omit_LAS_classes:
    thinning: 0

options:
  building_radius_vertex_elevation: 2.0
  radius_vertex_elevation: 1.0
  threshold_jump_edges: 0.5

output:
  format: {output_format}
  building_floor: true
  vertical_exaggeration: 0
        """.format(dbname=dbname,
                   host=host,
                   user=user,
                   pw=pw,
                   schema_tiles=schema_tiles,
                   bag_tile=bag_tile,
                   uniqueid=uniqueid,
                   pc_path=pc_dataset,
                   output_format=output_format)
    return(config)


def pc_name_dict(pc_dir, dataset_name):
    """Map dataset_dir to dataset_name
    
    Returns
    -------
    dict
        dataset_dir mapped to dataset_name
    """
    pc_name_map = {}
    if isinstance(pc_dir, list):
        for i, elem in enumerate(pc_dir):
            if isinstance(elem, str):
                pc_name_map[elem] = {'name': dataset_name[i], 'priority': i}
            else:
                for j, elem2 in enumerate(pc_dir[i]):
                    if isinstance(elem2, str):
                        pc_name_map[elem2] = {'name': dataset_name[i][j], 'priority': i}
                    else:
                        raise ValueError('Lists deeper than 2 levels are not supported in dataset_dir')
    else:
        pc_name_map[pc_dir] = {'name': dataset_name, 'priority': 0}
    logger.debug("pc_name_dict() out: %s", pformat(pc_name_map))
    return pc_name_map


def pc_file_index(pc_name_map):
    """Create an index table of the pointcloud files in the given directories
    
    Maps the location of the point cloud files to the point cloud tile IDs/names.
    The order of priority for the files is determined by the order in 
    'input_elevation: dataset_dir'
    And therefore, if given 5 directories [A, [B, C, D], E], their priority
    is [1, [2, 2, 2], 3]. Where a lower number means higher priority. Thus if a
    tile ('3a') has matching files (B/c3a.laz, C/B_3A.laz, D/o-3a.laz, E/a_3a.laz)
    in multiple directories (B, C, D, E), then the file in the directory with
    the higher priority is selected over that of lower priority. In this case
    the file E/a_3a.laz is disregarded. If directories are given in a sublist, such
    as B, C, D, then they have equal priority. Thus if a tile has a matching
    file in more than one of these directories, then *all* of the files are used 
    (B/c3a.laz, C/B_3A.laz, D/o-3a.laz).
    
    Tile name and file name matching is case insensitive. Furthermore, the match
    is governed by the file name patterns provided in 'input_elevation: dataset_name'.
    
    
    Returns
    -------
    dict
        {pc_tile_name: [path/to/pc_file1, path/to/pc_file2, ...]}
    """
    f_idx = {}
    pri = []
    file_index = {}
    
    def get_priority(d):
        return(d[1]['priority'])
    d_sort = sorted(pc_name_map.items(), key=get_priority)
    
    for elem in d_sort:
        idx = {}
        name = elem[1]['name']
        l = name[:name.find('{')]
        r = name[name.find('}')+1:]
        reg = '(?<=' + l + ').*(?=' + r + ')'
        t_pat = re.compile(reg, re.IGNORECASE)
        for item in os.listdir(elem[0]):
            path = os.path.join(elem[0], item)
            if os.path.isfile(path):
                pc_tile = t_pat.search(item)
                if pc_tile:
                    tile = pc_tile.group(0).lower()
                    idx[tile] = [path]
        f_idx[elem[0]] = idx
    # d_sort is [('/some/path', {'name': 'a_{tile}.laz', 'priority': 0}), ...]
    for d in reversed(d_sort):
        dirname = d[0]
        if len(pri) == 0:
            file_index = f_idx[dirname]
        else:
            if pri[-1] == d[1]['priority']:
                tiles = f_idx[dirname].keys()
                for t in tiles:
                    try:
                        file_index[t] += f_idx[dirname][t]
                    except KeyError:
                        file_index[t] = f_idx[dirname][t]
            else:
                f = {**file_index, **f_idx[dirname]}
                file_index = f
        pri.append(d[1]['priority'])
    logger.debug("pc_file_index() out: %s", file_index)
    return file_index


# def find_pc_files(pc_tiles, pc_dir, pc_dataset_name, pc_tile_case):
#     """Find pointcloud files in the file system when given a list of pointcloud tile names
#     """
#     tiles = format_tile_name(pc_tiles, pc_dataset_name, pc_tile_case)
#     
#     # use the tile list in tiles to parse the pointcloud file names
#     pc_path = [os.path.join(pc_dir, pc_tile) for pc_tile in tiles]
# 
#     if all([os.path.isfile(p) for p in pc_path]):
#         return(pc_path)
#     else:
#         return(None)
# 
# 
# def format_tile_name(pc_tiles, pc_dataset_name, pc_tile_case):
#     """Format the tile names according to input_elvation:dataset_name and :tile_case"""
#         # Prepare AHN file names -------------------------------------------------
#     if pc_tile_case == "upper":
#         tiles = [pc_dataset_name.format(tile=t.upper()) for t in pc_tiles]
#     elif pc_tile_case == "lower":
#         tiles = [pc_dataset_name.format(tile=t.lower()) for t in pc_tiles]
#     elif pc_tile_case == "mixed":
#         tiles = [pc_dataset_name.format(tile=t) for t in pc_tiles]
#     else:
#         raise "Please provide one of the allowed values for pc_tile_case."
#     
#     return tiles


def find_pc_tiles(db, table_index_pc, fields_index_pc,
                  table_index_footprint=None, fields_index_footprint=None,
                  extent_ewkb=None, tile_footprint=None,
                  prefix_tile_footprint=None):
    """Find pointcloud tiles in tile index that intersect the extent or the footprint tile.

    Parameters
    ----------
    prefix_tile_footprint : str or None
        Prefix prepended to the footprint tile view names. If None, the views are named as
        the values in fields_index_fooptrint['unit_name'].

    """
    if extent_ewkb:
        tiles = get_2Dtiles(db, table_index_pc, fields_index_pc, extent_ewkb)
    else:
        schema_pc_q = sql.Identifier(table_index_pc['schema'])
        table_pc_q = sql.Identifier(table_index_pc['table'])
        field_pc_geom_q = sql.Identifier(fields_index_pc['geometry'])
        field_pc_unit_q = sql.Identifier(fields_index_pc['unit_name'])

        schema_ftpr_q = sql.Identifier(table_index_footprint['schema'])
        table_ftpr_q = sql.Identifier(table_index_footprint['table'])
        field_ftpr_geom_q = sql.Identifier(fields_index_footprint['geometry'])
        field_ftpr_unit_q = sql.Identifier(fields_index_footprint['unit_name'])

        if prefix_tile_footprint:
            tile_footprint = tile_footprint.replace(
                prefix_tile_footprint, '', 1)

        tile_q = sql.Literal(tile_footprint)

        query = sql.SQL("""
                            SELECT
                            {table_pc}.{field_pc_unit}
                        FROM
                            {schema_pc}.{table_pc},
                            {schema_ftpr}.{table_ftpr}
                        WHERE
                            {table_ftpr}.{field_ftpr_unit} = {tile}
                            AND st_intersects(
                                {table_pc}.{field_pc_geom},
                                {table_ftpr}.{field_ftpr_geom}
                            );
                        """).format(table_pc=table_pc_q,
                                    field_pc_unit=field_pc_unit_q,
                                    schema_pc=schema_pc_q,
                                    schema_ftpr=schema_ftpr_q,
                                    table_ftpr=table_ftpr_q,
                                    field_ftpr_unit=field_ftpr_unit_q,
                                    tile=tile_q,
                                    field_pc_geom=field_pc_geom_q,
                                    field_ftpr_geom=field_ftpr_geom_q)

        resultset = db.getQuery(query)
        tiles = [tile[0].lower() for tile in resultset]
    logger.debug("find_pc_tiles() out: %s", tiles)
    return tiles


def extent_to_ewkb(db, table_index, file):
    """Reads a polygon from a file and returns its EWKB.

    I didn't find a simple way to safely get SRIDs from the input geometry
    with Shapely, therefore it is obtained from the database and the CRS of the
    polygon is assumed to be the same as of the tile indexes.

    Parameters
    ----------
    db : :py:class:`bag3d.config.db.db`
        Open connection
    table_index : dict
        {'schema' : str, 'table' : str} of the table of tile index.
    file : str
        Path to the polygon for clipping the input.
        Must be in the same CRS as the table_index.

    Returns
    -------
    [Shapely polygon, EWKB str]
    """
    schema = sql.Identifier(table_index['schema'])
    table = sql.Identifier(table_index['table'])

    query = sql.SQL("""SELECT st_srid(geom) AS srid
                    FROM {schema}.{table}
                    LIMIT 1;""").format(schema=schema, table=table)
    srid = db.getQuery(query)[0][0]

    assert srid is not None

    # Get clip polygon and set SRID
    with fiona.open(file, 'r') as src:
        poly = shape(src[0]['geometry'])
        # Change a the default mode to add this, if SRID is set
        geos.WKBWriter.defaults['include_srid'] = True
        # set SRID for polygon
        geos.lgeos.GEOSSetSRID(poly._geom, srid)
        ewkb = poly.wkb_hex

    return([poly, ewkb])


def get_2Dtiles(db, table_index, fields_index, ewkb):
    """Returns a list of tiles that overlap the output extent.

    Parameters
    ----------
    db : :py:class:`bag3d.config.db.db`
        Open connection
    table_index : dict
        {'schema' : str, 'table' : str} of the table of tile index.
    fields_index : dict
        {'primary_key' : str, 'geometry' : str, 'unit_name' : str}
        primary_key: Name of the primary_key field in table_index.
        geometry: Name of the geometry field in table_index.
        unit: Name of the field in table_index that contains the index unit names.
    ewkb : str
        EWKB representation of a polygon.

    Returns
    -------
    [tile IDs]
        Tiles that are intersected by the polygon that is provided in 'extent' (YAML).

    """
    schema = sql.Identifier(table_index['schema'])
    table = sql.Identifier(table_index['table'])
    field_idx_geom_q = sql.Identifier(fields_index['geometry'])
    field_idx_unit_q = sql.Identifier(fields_index['unit_name'])

    ewkb_q = sql.Literal(ewkb)
    # TODO: user input for a.unit
    query = sql.SQL("""
                SELECT {table}.{field_idx_unit}
                FROM {schema}.{table}
                WHERE st_intersects({table}.{field_idx_geom}, {ewkb}::geometry);
                """).format(schema=schema,
                            table=table,
                            field_idx_unit=field_idx_unit_q,
                            field_idx_geom=field_idx_geom_q,
                            ewkb=ewkb_q)
    resultset = db.getQuery(query)
    tiles = [tile[0] for tile in resultset]

    logger.debug("Nr. of tiles in clip extent: " + str(len(tiles)))

    return(tiles)


def get_2Dtile_area(db, table_index):
    """Get the area of a 2D tile.

    Note
    ----
    Assumes that all tiles have equal area. Area is in units of the tile CRS.

    Parameters
    ----------
    db : :py:class:`bag3d.config.db.db`
        Open connection
    table_index : list of str
        {'schema' : str, 'table' : str} of the table of tile index.

    Returns
    -------
    float

    """
    schema = sql.Identifier(table_index['schema'])
    table = sql.Identifier(table_index['table'])

    query = sql.SQL("""
                SELECT public.st_area(geom) AS area
                FROM {schema}.{table}
                LIMIT 1;
                """).format(schema=schema, table=table)
    area = db.getQuery(query)[0][0]

    return(area)


def get_2Dtile_views(db, schema_tiles, tiles):
    """Get View names of the 2D tiles. It tries to find views in schema_tiles
    that contain the respective tile ID in their name.

    Parameters
    ----------
    db : :py:class:`bag3d.config.db.db`
        Open connection
    schema_tiles: str
        Name of the schema where the 2D tile views are stored.
    tiles : list
        Tile IDs

    Returns
    -------
    list
        Name of the view that contain the tile ID as substring.

    """
    # Get View names for the tiles
    tstr = ["%" + str(tile) + "%" for tile in tiles]
    t = sql.Literal(tstr)
    schema_tiles = sql.Literal(schema_tiles)
    query = sql.SQL("""SELECT table_name
                        FROM information_schema.views
                        WHERE table_schema = {}
                        AND table_name LIKE any({});
                        """).format(schema_tiles, t)
    resultset = db.getQuery(query)
    tile_views = [tile[0] for tile in resultset]
    if tile_views:
        return tile_views
    else:
        logger.error("get_2Dtile_views returned None with %s", 
                      query.as_string(db.conn))


def clip_2Dtiles(db, user_schema, schema_tiles, tiles, poly, clip_prefix,
                 fields_view):
    """Creates views for the clipped tiles.

    Parameters
    ----------
    db : :py:class:`bag3d.config.db.db`
        Open connection
    user_schema: str
    schema_tiles : str
    tiles : list
    poly : Shapely polygon
    clip_prefix : str

    Returns
    -------
    list
        Name of the views of the clipped tiles.

    """
    user_schema = sql.Identifier(user_schema)
    schema_tiles = sql.Identifier(schema_tiles)
    tiles_clipped = []

    fields_all = fields_view['all']
    field_geom_q = sql.Identifier(fields_view['geometry'])

    for tile in tiles:
        t = clip_prefix + tile
        tiles_clipped.append(t)
        view = sql.Identifier(t)
        tile_view = sql.Identifier(tile)
        fields_q = parse_sql_select_fields(tile, fields_all)
        wkb = sql.Literal(poly.wkb_hex)
        query = sql.SQL("""
            CREATE OR REPLACE VIEW {user_schema}.{view} AS
                SELECT
                    {fields}
                FROM
                    {schema_tiles}.{tile_view}
                WHERE
                    st_within({tile_view}.{geom}, {wkb}::geometry)"""
                        ).format(user_schema=user_schema,
                                 schema_tiles=schema_tiles,
                                 view=view,
                                 fields=fields_q,
                                 tile_view=tile_view,
                                 geom=field_geom_q,
                                 wkb=wkb)
        db.sendQuery(query)
    try:
        db.conn.commit()
        print(
            str(
                len(tiles_clipped)) +
            " views with prefix '{}' are created in schema {}.".format(
                clip_prefix,
                user_schema))
    except BaseException:
        print("Cannot create view {user_schema}.{clip_prefix}{tile}".format(
            schema_tiles=schema_tiles, clip_prefix=clip_prefix))
        db.conn.rollback()

    return(tiles_clipped)


def union_2Dtiles(db, user_schema, tiles_clipped, clip_prefix, fields_view):
    """Union the clipped tiles into a single view.

    Parameters
    ----------
    db : :py:class:`bag3d.config.db.db`
        Open connection
    user_schema : str
    tiles_clipped : list
    clip_prefix : str

    Returns
    -------
    str
        Name of the united view.
    """
    # Check if there are enough tiles to unite
    assert len(tiles_clipped) > 1, "Need at least 2 tiles for union"

    user_schema = sql.Identifier(user_schema)
    u = "{clip_prefix}union".format(clip_prefix=clip_prefix)
    union_view = sql.Identifier(u)
    sql_query = sql.SQL("CREATE OR REPLACE VIEW {user_schema}.{view} AS ").format(
        user_schema=user_schema, view=union_view)

    fields_all = fields_view['all']

    for tile in tiles_clipped[:-1]:
        view = sql.Identifier(tile)
        fields_q = parse_sql_select_fields(tile, fields_all)
        sql_subquery = sql.SQL("""SELECT {fields}
                               FROM {user_schema}.{view}
                               UNION ALL """).format(fields=fields_q,
                                                     user_schema=user_schema,
                                                     view=view)
        sql_query = sql_query + sql_subquery
    # The last statement
    tile = tiles_clipped[-1]
    view = sql.Identifier(tile)
    fields_q = parse_sql_select_fields(tile, fields_all)
    sql_subquery = sql.SQL("""SELECT {fields}
                           FROM {user_schema}.{view};
                           """).format(fields=fields_q,
                                       user_schema=user_schema,
                                       view=view)
    sql_query = sql_query + sql_subquery
    logger.debug(db.print_query(sql_query))
    db.sendQuery(sql_query)
    try:
        db.conn.commit()
        print("View {} created in schema {}.".format(u, user_schema))
    except BaseException:
        print("Cannot create view {}.{}".format(user_schema, u))
        db.conn.rollback()
        return(False)
    return u


def get_view_fields(db, user_schema, tile_views):
    """Get the fields in a 2D tile view

    Parameters
    ----------
    tile_views : list of str

    Returns
    -------
    {'all' : list, 'geometry' : str}

    """
    if len(tile_views) > 0:
        schema_q = sql.Literal(user_schema)
        view_q = sql.Literal(tile_views[0])

        resultset = db.getQuery(sql.SQL("""
                            SELECT
                                column_name
                            FROM
                                information_schema.columns
                            WHERE
                                table_schema = {schema}
                                AND table_name = {view};
                            """).format(schema=schema_q,
                                        view=view_q))
        f = [field[0] for field in resultset]

        geom_res = db.getQuery(sql.SQL("""
                            SELECT
                                f_geometry_column
                            FROM
                                public.geometry_columns
                            WHERE
                                f_table_schema = {schema}
                                AND f_table_name = {view};
                            """).format(schema=schema_q,
                                        view=view_q))
        f_geom = geom_res[0][0]

        fields = {}
        fields['all'] = f
        fields['geometry'] = f_geom
        return(fields)
    else:
        return None


def parse_sql_select_fields(table, fields):
    """Parses a list of field names into "table"."field" to insert into a SELECT ... FROM table

    Parameters
    ----------
    fields : list of str

    Returns
    -------
    psycopg2.sql.Composable

    """
    s = []
    for f in fields:
        s.append(sql.SQL('.').join([sql.Identifier(table), sql.Identifier(f)]))
    sql_fields = sql.SQL(', ').join(s)
    return sql_fields


def drop_2Dtiles(db, user_schema, views_to_drop):
    """Drops Views in a given schema.

    Note
    ----
    Used for dropping the views created by clip_2Dtiles() and union_2Dtiles().

    Parameters
    ----------
    db : :py:class:`bag3d.config.db.db`
        Open connection
    user_schema : str
    views_to_drop : list

    Returns
    -------
    bool
        True on success, False on failure

    """
    user_schema = sql.Identifier(user_schema)

    for view in views_to_drop:
        view = sql.Identifier(view)
        queries = sql.Composed('')
        query = sql.SQL("DROP VIEW IF EXISTS {user_schema}.{view} CASCADE;").format(
            user_schema=user_schema, view=view)
        queries += query
    logger.debug(db.print_query(queries))
    try:
        db.sendQuery(queries)
        logger.debug("Dropped {} in schema {}.".format(views_to_drop, user_schema))
        return True
    except BaseException as e:
        logger.error("Cannot drop views ", views_to_drop)
        logger.exception(e)
        return False


def configure_tiles(conn, config, clip_prefix):
    """Configure the tile list based on the input parameter
    
    Returns
    -------
    dict
        The configuration with the configured tile list. The following elements
        are added to the config:
        - `clip_prefix` : 
        - `tile_out` : name of the tile
        - extent_ewkb : ewkb of the extent polygon
        - tile_list is overwritten, either based on the provided extent or 
        the provided tile names are substituted with the names of the tile 
        views in input_polygons:tile_schema
    """
    config["clip_prefix"] = clip_prefix
    config["tile_out"] = None
    config["extent_ewkb"] = None
    # TODO: assert that CREATE/DROP allowed on TILE_SCHEMA and/or USER_SCHEMA
    if config["input_polygons"]["extent"]:
        poly, ewkb = extent_to_ewkb(conn, config['tile_index']['polygons'], 
                                    config["input_polygons"]["extent"])
        config["extent_ewkb"] = ewkb
        tiles = get_2Dtiles(conn, config['tile_index']['polygons'], 
                            config['tile_index']['polygons']['fields'], ewkb)
        # Get view names for tiles
        tile_views = get_2Dtile_views(conn, 
                                      config["input_polygons"]["tile_schema"], 
                                      tiles)
        view_fields = get_view_fields(conn, 
                                      config["input_polygons"]["tile_schema"], 
                                      tile_views)
        # clip 2D tiles to extent
        tiles_clipped = clip_2Dtiles(conn, 
                                     config["input_polygons"]['user_schema'], 
                                     config["input_polygons"]['tile_schema'], 
                                     tile_views, poly, clip_prefix, view_fields)

        # if the area of the extent is less than that of a tile, union the tiles is the
        # extent spans over many
        tile_area = get_2Dtile_area(conn, config['tile_index']['polygons'])
        if len(tiles_clipped) > 1 and poly.area < tile_area:
            union_view = union_2Dtiles(conn, 
                                       config["input_polygons"]['user_schema'], 
                                       tiles_clipped, clip_prefix, view_fields)
            config["tile_out"] = "output_batch3dfier"
            config["input_polygons"]["tile_list"] = [union_view]
        else:
            config["input_polygons"]["tile_list"] = tiles_clipped

    elif config["input_polygons"]["tile_list"]:
        if 'all' in config["input_polygons"]["tile_list"]:
            poly = config['tile_index']['polygons']
            schema_q = sql.Identifier(poly['schema'])
            table_q = sql.Identifier(poly['table'])
            unit_q = sql.Identifier(poly['fields']['unit_name'])
            
            query = sql.SQL("SELECT {unit} FROM {schema}.{table};").format(
                schema=schema_q, table=table_q, unit=unit_q)
            tiles = [tile[0] for tile in conn.getQuery(query)]
            tile_views = get_2Dtile_views(conn, 
                                          config["input_polygons"]['tile_schema'],
                                          tiles)
        else:
            tile_views = get_2Dtile_views(conn, 
                                          config["input_polygons"]['tile_schema'],
                                          config["input_polygons"]["tile_list"])
        if not tile_views or len(tile_views) == 0:
            logger.error("tile_views is None or len(tile_views) == 0")
        else:
            config["input_polygons"]["tile_list"] = tile_views

    else:
        raise TypeError("Please provide either 'extent' or 'tile_list' in config.")
    return config
