# -*- coding: utf-8 -*-

"""Database connection class."""

import psycopg2
import logging

class db(object):
    """A database connection class """

    def __init__(self, dbname, host, port, user, password):
        self.dbname = dbname
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        try:
            self.conn = psycopg2.connect(
                "dbname=%s host=%s port=%s user=%s password=%s" %
                (dbname, host, port, user, password))
            logging.debug("Opened database successfully")
        except BaseException as e:
            logging.exception("I'm unable to connect to the database. Exiting function.")

    def sendQuery(self, query):
        """Send a query to the DB when no results need to return (e.g. CREATE)

        Parameters
        ----------
        query : str


        Returns
        -------
        nothing

        """
        with self.conn:
            with self.conn.cursor() as cur:
                cur.execute(query)

    def getQuery(self, query):
        """DB query where the results need to return (e.g. SELECT)

        Parameters
        ----------
        query : str
            SQL query


        Returns
        -------
        psycopg2 resultset

        """
        with self.conn:
            with self.conn.cursor() as cur:
                cur.execute(query)
                return cur.fetchall()

    def vacuum(self, schema, table):
        """Vacuum analyze a table

        Parameters
        ----------
        schema : str
            schema name
        table : str
            table name

        Returns
        -------
        nothing
        """
        self.conn.set_isolation_level(
            psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        schema = psycopg2.sql.Identifier(schema)
        table = psycopg2.sql.Identifier(table)
        query = psycopg2.sql.SQL("""
        VACUUM ANALYZE {schema}.{table};
        """).format(schema=schema, table=table)
        self.sendQuery(query)
    
    def vacuum_full(self):
        """Vacuum analyze the whole database"""
        self.conn.set_isolation_level(
            psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        query = psycopg2.sql.SQL("VACUUM ANALYZE;")
        self.sendQuery(query)

    def close(self):
        """Close connection"""
        self.conn.close()
        logging.debug("Closed database successfuly")


def create():
    """Create a BAG database from the BAG extract"""
