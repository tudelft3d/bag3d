# -*- coding: utf-8 -*-

"""Update the BAG database (2D) and tile index"""

import os.path
from datetime import datetime, date
from subprocess import PIPE
from psutil import Popen, Process, NoSuchProcess, ZombieProcess, AccessDenied, swap_memory, virtual_memory
import locale
from shutil import which

# from memory_profiler import memory_usage

import logging
from bs4 import BeautifulSoup
import urllib.request
from psycopg2 import sql

from bag3d.config import db


logger = logging.getLogger(__name__)
logger_perf = logging.getLogger('performance')


# def report_procs(pid):
#     proc = Process(pid)
#     try:
#         with proc.oneshot():
#             logger_perf.debug("%s - %s - %s - %s" % (proc.cmdline(), proc.cpu_percent(), proc.memory_full_info(), swap_memory()))
#     except NoSuchProcess:
#         pass
    #
    # res = []
    # for p in psutil.process_iter(attrs=['name', 'status', "cmdline", 'memory_info']):
    #     if '3dfier' in p.info['name']:
    #         res.append((p.info['cmdline'], p.info['memory_info']))
    # if len(res) > 0:
    #     res.extend(["loadavg %s" % str(os.getloadavg()), psutil.swap_memory()])
    #     return res
    # else:
    #     return None


def run_subprocess(command, shell=False, doexec=True, monitor=False, tile_id=None):
    """Subprocess runner
    
    If subrocess returns non-zero exit code, STDERR is sent to the logger.
    
    Parameters
    ----------
    command : list of str
        Command to pass to subprocess.run(). Eg ['wget', '-q', '-r', dl_url]
    shell : bool
        Passed to subprocess.run()
    doexec : bool
        Execute the subprocess or just print out the concatenated command
    
    Returns
    -------
    nothing
        nothing
    """
    if doexec:
        cmd = " ".join(command)
        if shell:
            command = cmd
        logger.debug(command)
        popen = Popen(command, shell=shell, stderr=PIPE, stdout=PIPE)
        pid = popen.pid
        if monitor:
            proc = Process(pid)
            with proc.oneshot():
                try:
                    logger_perf.debug("%s;%s;%s" % (
                        tile_id, virtual_memory().used, swap_memory().used))
                except NoSuchProcess or ZombieProcess:
                    logger.debug("%s is Zombie or NoSuchProcess" % tile_id)
                except AccessDenied as e:
                    logger_perf.exception(e)
        # if monitor:
        #     running = True
        #     proc = Process(pid)
        #     with proc.oneshot():
        #         while running:
        #             try:
        #                 logger_perf.debug("%s - %s - %s - %s - %s" % (
        #                 tile_id, proc.cpu_percent(), proc.cpu_times(), proc.memory_full_info(), swap_memory()))
        #             except NoSuchProcess or ZombieProcess:
        #                 logger.debug("%s is Zombie or NoSuchProcess" % tile_id)
        #                 break
        #             except AccessDenied as e:
        #                 logger_perf.exception(e)
        #                 break
        #             running = proc.is_running()
        #             logger.debug("%s is running: %s" % (tile_id, running))
        #             sleep(1)
        stdout, stderr = popen.communicate()
        err = stderr.decode(locale.getpreferredencoding(do_setlocale=True))
        popen.wait()
        if popen.returncode != 0:
            logger.debug("Process returned with non-zero exit code: %s", popen.returncode)
            logger.error(err)
            return False
        else:
            return True
    else:
        logger.debug("Not executing %s", command)
        return True


def get_latest_BAG(url):
    """Get the date of the latest BAG extract from NLExtract
    
    Parameters
    ----------
    url : str
        URL to the BAG extract eg http://data.nlextract.nl/bag/postgis/
    
    Returns
    -------
    datetime.date
        Date of the latest available BAG extract
    """
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
    """Prepares the BAG database
    
    Creates the *public.bag_updates* table and *tile_index* schema.
    
    Parameters
    ----------
    conn : :py:class:`bag3d.config.db.db`
        Open connection
    doexec : bool
        Passed to :py:func:`run_subprocess`
        
    Returns
    -------
    nothing
        nothing
    """
    conn.check_postgis()
    query = sql.SQL("""
CREATE TABLE IF NOT EXISTS public.bag_updates (id serial constraint id_pkey primary key, last_update timestamp, note text);
CREATE SCHEMA IF NOT EXISTS tile_index;
""")
    if doexec:
        logger.debug(conn.print_query(query))
        conn.sendQuery(query)
    else:
        logger.debug(conn.print_query(query))


def run_pg_restore(dbase, dump=None, doexec=True):
    """Run the pg_restore process
    
    Parameters
    ----------
    dbase : dict
        Dict containing the database connection parameters from the config file
    doexec : bool
        Passed to :py:func:`run_subprocess`
    
    Returns
    -------
    nothing
        nothing
    """
    # Drop the schema first in order to restore
    command = ['psql', '-h', dbase['host'], '-U', dbase['user'],
               '-d', dbase['dbname'], '-w', '-c',
               "'DROP SCHEMA IF EXISTS bagactueel CASCADE;'"]
    run_subprocess(command, shell=True, doexec=doexec)
    
    # Restore from the latest extract
    command = ['/usr/lib/postgresql/10/bin/pg_restore', '--no-owner', '--no-privileges', '-j', '20',
               '-h', dbase['host'], '-U', dbase['user'], '-d', dbase['dbname'],
               dump]
    run_subprocess(command, doexec=doexec)


def download_BAG(url, doexec=True):
    """Download the latest BAG extract
    
    Parameters
    ----------
    url : str
        URL to the BAG extract eg http://data.nlextract.nl/bag/postgis/
    doexec : bool
        Passed to :py:func:`run_subprocess`
    
    Returns
    -------
    nothing
        nothing
    """
    is_wget = which("wget")
    if is_wget is None:
        logger.error("'wget' not found, exiting")
        exit(1)
    dl_url = os.path.join(url, 'bag-laatst.backup')
    command = ['wget', '-q', '-r', dl_url] 
    run_subprocess(command, doexec=doexec)


def restore_BAG(dbase, bag_latest=None, dump=None, doexec=True):
    """Restores the BAG extract into a database
    
    Parameters
    ----------
    dbase : dict
        Dict containing the database connection parameters from the config file
    doexec : bool
        Passed to :py:func:`run_subprocess`
    
    Returns
    -------
    bool
        True on success, False on failure
    """
    try:
        conn = db.db(dbname=dbase['dbname'], host=dbase['host'],
                  port=dbase['port'], user=dbase['user'], 
                  password=dbase['pw'])
    except BaseException:
        raise
    
    setup_BAG(conn, doexec=doexec)

    if dump is None:
        bag_url = 'http://data.nlextract.nl/bag/postgis/'
        bag_latest = get_latest_BAG(bag_url)
        logger.debug("bag_latest is %s", bag_latest.isoformat())
    else:
        bag_latest = datetime.strptime(bag_latest, '%Y-%m-%d').date()
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
        if dump is None:
            logger.info("There is a newer BAG-extract available, starting download and update...")
            download_BAG(bag_url, doexec=doexec)
        
        run_pg_restore(dbase, dump=dump, doexec=doexec)
        
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
                logger.debug("Updated bag_updates and commented on bagactueel schema.")
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


def import_index(idx, dbname, tile_schema, host, port, user,
                 pw=None, doexec=True):
    """Import the tile index into the database
    
    Calls ogr2ogr to import a tile index with EPSG:28992 into the tile_index
    schema.
    """
    if pw:
        pg_conn = 'PG:"dbname={d} host={h} port={p} user={u} password={pw}"'.format(
        d=dbname, h=host, p=port, u=user, pw=pw)
    else:
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


def grant_access(conn, user, tile_schema, tile_index_schema, production_schema):
    """Grants all the necessary privileges for a user for operating on the 3DBAG database
    
    Parameters
    ----------
    conn : :py:class:`bag3d.config.db.db`
        Open connection
    user : str
        User to grant the privileges to
    tile_schema : str
        Schema with the footprint tiles, in config: 'input_polygons:tile_schema'
    tile_index_schema : str
        Schema with the footprint tile index, in config: 'tile_index:polygons:schema'
    
    Returns
    -------
    nothing
        nothing
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

    query_bagactueel = sql.SQL("""
    GRANT USAGE ON SCHEMA {s} TO {u};
    GRANT SELECT ON ALL tables IN SCHEMA {s} TO {u};
    GRANT USAGE, SELECT ON ALL sequences IN SCHEMA {s} TO {u};
    """).format(u=sql.Identifier(user), s=sql.Identifier(production_schema))
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
