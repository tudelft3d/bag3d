# -*- coding: utf-8 -*-

"""Update the BAG database (2D) and tile index"""

import os.path
from datetime import datetime, date
from subprocess import run, PIPE
import locale

import logging
from bs4 import BeautifulSoup
import urllib.request
from psycopg2 import sql

from bag3d.config import db


logger = logging.getLogger('update.bag')


def run_subprocess(command, shell=False, doexec=True):
    """Subprocess runner"""
    if doexec:
        cmd = " ".join(command)
        logger.debug(cmd)
        if shell:
            command = cmd
        proc = run(command, shell=shell, stderr=PIPE, stdout=PIPE)
        err = proc.stderr.decode(locale.getpreferredencoding(do_setlocale=True))
        if proc.returncode != 0:
            logger.error("Process returned with non-zero exit code")
            logger.error(err)
    else:
        cmd = " ".join(command)
        logger.debug(cmd)


def get_latest_BAG(url):
    """Get the date of the latest BAG extract from NLExtract"""
    r = urllib.request.urlopen(url).read()
    soup = BeautifulSoup(r, "lxml")
    
    data = {}
    table = soup.find('table')
    
    rows = table.find_all('tr')
    for row in rows:
        cols = row.find_all('td')
        cols = [ele.text.strip() for ele in cols]
        if cols and len(cols[2]) > 1:
            date = datetime.strptime(cols[2], "%Y-%m-%d %H:%M").date()
            data[cols[1]] = date
        else:
            pass
    return data['bag-laatst.backup']


def setup_BAG(conn, doexec=True):
    """Prepares the BAG database"""
    conn.check_postgis()
    query = sql.SQL("""
CREATE TABLE public.bag_updates (id serial constraint id_pkey primary key, last_update timestamp, note text);
CREATE SCHEMA tile_index;
""")
    if doexec:
        logger.debug(conn.print_query(query))
        conn.sendQuery(query)
    else:
        logger.debug(conn.print_query(query))


def run_pg_restore(dbase, doexec=True):
    """Run the pg_restore process"""
    # Drop the schema first in order to restore
    command = ['psql', '-h', dbase['host'], '-U', dbase['user'],
               '-d', dbase['dbname'], '-w', '-c',
               "'DROP SCHEMA IF EXISTS bagactueel CASCADE;'"]
    run_subprocess(command, doexec=doexec)
    
    # Restore from the latest extract
    command = ['pg_restore', '--no-owner', '--no-privileges', '-j', '20',
               '-h', dbase['host'], '-U', dbase['user'], '-d', dbase['dbname'],
               '-w', './data.nlextract.nl/bag/postgis/bag-laatst.backup']
    run_subprocess(command, doexec=doexec)


def download_BAG(url, doexec=True):
    """Download the latest BAG extract"""
    dl_url = os.path.join(url, 'bag-laatst.backup')
    command = ['wget', '-q', '-r', dl_url] 
    run_subprocess(command, doexec=doexec)

def restore_BAG(dbase, doexec=True):
    """Restores the BAG extract into a database"""
    try:
        conn = db.db(dbname=dbase['dbname'], host=dbase['host'],
                  port=dbase['port'], user=dbase['user'], 
                  password=dbase['pw'])
    except BaseException:
        raise
    
    setup_BAG(conn, doexec=doexec)
    
    bag_url = 'http://data.nlextract.nl/bag/postgis/'
    bag_latest = get_latest_BAG(bag_url)
    logger.debug("bag_latest is %s", bag_latest.isoformat())
    
    # Get the date of the last update on the BAG database on Godzilla ----------
    query = "SELECT max(last_update) FROM public.bag_updates;"
    godzilla_update = conn.getQuery(query)[0][0]
    
    # in case there is no entry yet in last_update
    if godzilla_update:
        logger.debug("public.bag_updates is not empty")
        godzilla_update = godzilla_update.date()
    else:
        logger.debug("public.bag_updates is empty")
        godzilla_update = date(1,1,1)
    logger.debug("godzilla_update is %s", godzilla_update.isoformat())
    
    # Download the latest dump if necessary ------------------------------------
    if bag_latest > godzilla_update:
        logger.info("There is a newer BAG-extract available, starting download and update...")
        download_BAG(bag_url, doexec=doexec)
        
        run_pg_restore(dbase, doexec=doexec)
        
        # Update timestamp in bag_updates
        query = sql.SQL("""
INSERT INTO public.bag_updates (last_update, note)
VALUES ({}, 'auto-update by overwriting the bagactueel schema');
        """).format(sql.Literal(bag_latest))
#         logger.debug(query.as_string(conn.conn).strip().replace('\n', ' '))
        logger.debug(conn.print_query(query))
        
        if doexec:
            conn.sendQuery(query)
            conn.sendQuery("""
            COMMENT ON SCHEMA bagactueel IS 
            '!!! WARNING !!! This schema contains the BAG itself.
             At every update, there is a DROP SCHEMA bagactueel CASCADE,
            which deletes the schema with all its contents and all objects
            depending on the schema. Therefore you might want to save
            your scripts to recreate the views etc. that depend on
            this schema, otherwise they will be lost forever.';
            """)
            try:
                conn.conn.commit()
                logger.debug("\nUpdated bag_updates and commented on bagactueel schema.\n")
                return True
            except:
                conn.conn.rollback()
                logger.error("""Cannot update public.bag_updates and/or comment on
                 schema bagactueel. Rolling back transaction""")
                return False
            finally:
                command = ['rm', '-r', '-f', './data.nlextract.nl']
                run_subprocess(command, doexec=doexec)
                conn.close()
        else:
            logger.debug("Not executing commands")
            conn.close()
    else:
        logger.info("Godzilla is up-to-date with the BAG.")
        conn.close()
        return False


def import_index(idx, dbname, tile_schema, host, port, user, doexec=True):
    """Import the tile index into the database
    
    Calls ogr2ogr.
    """
    pg_conn = 'PG:"dbname={d} host={h} port={p} user={u}"'.format(
        d=dbname, h=host, p=port, u=user)
    schema = 'SCHEMA=%s' % tile_schema
    i = os.path.abspath(idx)
    command = ['ogr2ogr', '-f', 'PostgreSQL', pg_conn, i, 
               '-skip-failure', '-a_srs', 'EPSG:28992', 
               '-lco', 'OVERWRITE=yes',
               '-lco', schema,
               '-lco', 'FID=id', 
               '-lco', 'GEOMETRY_NAME=geom']
    run_subprocess(command, shell=True, doexec=doexec)


def grant_access(conn, user, tile_schema, tile_index_schema):
    """Grants all the necessary privileges for a user for operating on the 3DBAG database
    
    Parameters
    ----------
    conn: db Class connection
    tile_schema: Schema with the footprint tiles, in config: 'input_polygons:tile_schema'
    tile_index_schema: Schema with the footprint tile index, in config: 'tile_index:polygons:schema'
    """
    query_public = sql.SQL("""
GRANT EXECUTE ON ALL functions IN SCHEMA public TO {u};
GRANT SELECT ON ALL tables IN SCHEMA public TO {u};
GRANT USAGE, SELECT ON ALL sequences IN SCHEMA public TO {u};
""").format(u=sql.Identifier(user))
    logger.debug(conn.print_query(query_public))
    conn.sendQuery(query_public)
    
    query_bagactueel = sql.SQL("""
GRANT USAGE ON SCHEMA bagactueel TO {u};
GRANT SELECT ON ALL tables IN SCHEMA bagactueel TO {u};
GRANT USAGE, SELECT ON ALL sequences IN SCHEMA bagactueel TO {u};
""").format(u=sql.Identifier(user))
    logger.debug(conn.print_query(query_bagactueel))
    conn.sendQuery(query_bagactueel)
    
    query_tile_schema = sql.SQL("""
GRANT USAGE ON SCHEMA {s} TO {u};
GRANT SELECT, UPDATE, INSERT ON ALL tables IN SCHEMA {s} TO {u};
GRANT USAGE, SELECT, UPDATE ON ALL sequences IN SCHEMA {s} TO {u};
""").format(u=sql.Identifier(user), s=sql.Identifier(tile_schema))
    logger.debug(conn.print_query(query_tile_schema))
    conn.sendQuery(query_tile_schema)

    query_tile_index_schema = sql.SQL("""
GRANT USAGE ON SCHEMA {s} TO {u};
GRANT SELECT, UPDATE, INSERT ON ALL tables IN SCHEMA {s} TO {u};
GRANT USAGE, SELECT, UPDATE ON ALL sequences IN SCHEMA {s} TO {u};
""").format(u=sql.Identifier(user), s=sql.Identifier(tile_index_schema))
    logger.debug(conn.print_query(query_tile_index_schema))
    conn.sendQuery(query_tile_index_schema)
