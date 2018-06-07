from datetime import date
import os.path

import pytest
import logging

from bag3d.update import bag

@pytest.fixture('module')
def bag_url():
    yield 'http://data.nlextract.nl/bag/postgis/'

@pytest.fixture('module')
def dbname():
    yield {
    'dbname': 'batch3dfier_db',
    'host': 'localhost',
    'port': 5432,
    'pw': None,
    'user': 'batch3dfier'
    }


class TestBAG():
    """Testing the BAG module"""
    def test_get_latest_bag(self, bag_url):
        d = bag.get_latest_BAG(bag_url)
        assert isinstance(d, date)
    
    def test_run_pg_restore(self, caplog, dbname):
        with caplog.at_level(logging.DEBUG):
            bag.run_pg_restore(dbname, doexec=False)
    
    def test_download_BAG(self, caplog, bag_url):
        with caplog.at_level(logging.DEBUG):
            bag.download_BAG(bag_url, doexec=False)
    
    def test_restore_BAG(self, caplog, dbname):
        with caplog.at_level(logging.DEBUG):
            bag.restore_BAG(dbname, doexec=False)
    
    def test_import_index(self, caplog, doexec=True):
        with caplog.at_level(logging.DEBUG):
            p = os.path.join(os.getcwd(), 'example_data', 'bag_index_test.json')
            bag.import_index(p, 
                             'batch3dfier_db', 
                             'tile_index', 
                             'localhost', 
                             '5432', 
                             'batch3dfier', doexec)