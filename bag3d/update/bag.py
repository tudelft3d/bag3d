# -*- coding: utf-8 -*-

"""Update the BAG database (2D) and tile index"""

from datetime import datetime
from subprocess import run, PIPE
import locale

import logging
from bs4 import BeautifulSoup
import urllib.request
from psycopg2 import sql

from bag3d.config import db


logger = logging.getLogger('update.bag')


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


def setup_BAG(conn):
    """Prepares the BAG database"""
    conn.check_postgis()
    conn.sendQuery("""
    CREATE TABLE public.bag_updates (id serial constraint id_pkey primary key, last_update timestamp, note text);
    CREATE SCHEMA tile_index;
    """)


def run_subprocess(command):
    """Subprocess runner"""
    proc = run(command, stderr=PIPE, stdout=PIPE)
    err = proc.stderr.decode(locale.getpreferredencoding(do_setlocale=True))
    if proc.returncode != 0:
        logger.error("Process returned with non-zero exit code", err)

def run_pg_restore(dbase, doexec=True):
    """Run the pg_restore process"""
    # Drop the schema first in order to restore
    command = ['psql', '-h', dbase['host'], '-U', dbase['user'],
               '-d', dbase['dbname'], '-w', '-c',
               "'DROP SCHEMA IF EXISTS bagactueel CASCADE;'"]
    if doexec:
        run_subprocess(command)
    else:
        logger.debug(" ".join(command))
    
    # Restore from the latest extract
    command = ['pg_restore', '--no-owner', '--no-privileges', '-j', '20',
               '-h', dbase['host'], '-U', dbase['user'], '-d', dbase['dbname'],
               '-w', './data.nlextract.nl/bag/postgis/bag-laatst.backup']
    if doexec:
        run_subprocess(command)
    else:
        logger.debug(" ".join(command))


def download_BAG(url, doexec=True):
    """Download the latest BAG extract"""
    command = ['wget', '-q', '-r', url] 
    if doexec:
        run_subprocess(command)
    else:
        logger.debug(" ".join(command))

def restore_BAG(dbase):
    """Restores the BAG extract into a database"""
    try:
        conn = db.db(dbname=dbase['dbname'], host=dbase['host'],
                  port=dbase['port'], user=dbase['user'], 
                  password=dbase['pw'])
    except BaseException:
        raise
    
    setup_BAG(conn)
    
    bag_url = 'http://data.nlextract.nl/bag/postgis/'
    bag_latest = get_latest_BAG(bag_url)
    logger.debug("bag_latest is %s", bag_latest.isoformat())
    
    # Get the date of the last update on the BAG database on Godzilla ----------
    query = "SELECT max(last_update) FROM public.bag_updates;"
    godzilla_update = conn.getQuery(query)[0][0]
    
    # in case there is no entry yet in last_update
    if godzilla_update:
        godzilla_update = godzilla_update.date()
    else:
        godzilla_update = datetime.date(1,1,1)
    logger.debug("godzilla_update is %s", godzilla_update.isoformat())
    
    # Download the latest dump if necessary ------------------------------------
    if bag_latest > godzilla_update:
        logger.info("There is a newer BAG-extract available, starting download and update...\n")
        download_BAG(bag_url, doexec=False)
        
        run_pg_restore(dbase, doexec=False)
        
        # Update timestamp in bag_updates
        query = sql.SQL("""INSERT INTO public.bag_updates (last_update, note)
                VALUES ({}, 'auto-update by overwriting the bagactueel schema');
                """).format(sql.Literal(bag_latest))
        conn.sendQuery(query)
        
        conn.sendQuery("""COMMENT ON SCHEMA bagactueel IS 
            '!!! WARNING !!! This schema contains the BAG itself.
             At every update, there is a DROP SCHEMA bagactueel CASCADE,
              which deletes the schema with all its contents and all objects
               depending on the schema. Therefore you might want to save
                your scripts to recreate the views etc. that depend on
                 this schema, otherwise they will be lost forever.';""")
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
            # delete backup file
            command = "rm -rf ./data.nlextract.nl"
            run(command, shell=True)
    
    else:
        logger.info("Godzilla is up-to-date with the BAG.")
        return False
    
    conn.close()