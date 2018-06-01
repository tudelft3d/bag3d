import pytest
import os

import yaml
import pykwalify.errors

from bag3d.config import args
from bag3d.config import db


@pytest.fixture(scope='module')
def invalid_config(request):
    d = {'bag3d_table': 'bag3d',
 'database': {'dbname': 'batch3dfier_db',
              'host': 'localhost',
              'port': 5432,
              'pw': 'batch3d_test',
              'user': 'batch3dfier'},
 'elevation': {'border_table': None,
               'fields': {'geometry': 'geom',
                          'primary_key': 'gid',
                          'unit_name': 'unit'},
               'schema': 'tile_index',
               'table': 'ahn_index'},
 'extent_file': '/home/bdukai/Development/batch3dfier/example_data/extent_small.geojson',
 'out_schema': 'bagactueel',
 'out_table': 'heights',
 'output_dir': '/tmp/3DBAG',
 'path_3dfier': '/home/bdukai/Development/3dfier/build/3dfier',
 'pc_dataset_name': ['c_{tile}.laz', '{tile}.laz'],
 'pc_dir': ['/example_data/ahn3', '/example_data/ahn2/merged'],
 'polygons': {'fields': {'geometry': 'geom',
                         'primary_key': 'gid',
                         'unit_name': 'unit'},
              'schema': 'tile_index',
              'table': 'bag_index'},
 'prefix_tile_footprint': 't_',
 'tile_schema': 'bag_tiles',
 'tiles': None,
 'uniqueid': 'identification',
 'user_schema': 'bag_tiles'}
    
    fname = 'invalid_conf.yml'
    with open(fname, 'w') as f:
        yaml.dump(d, f)
    def del_conf():
        os.remove('invalid_conf.yml')
    request.addfinalizer(del_conf)
    return fname


class TestArgs():
    """Testing config.args"""
    def test_config_validation(self, config, schema):
        """Schema validation"""
        try:
            args.validate_config(config, schema)
        except pykwalify.errors.PyKwalifyException:
            raise
    
    def test_config_invalid(self, invalid_config, schema):
        """Invalid schema"""
        with pytest.raises(pykwalify.errors.PyKwalifyException):
            args.validate_config(invalid_config, schema)
    def test_schema_notfound(self, config, schema='some_schema.yml'):
        """Invalid schema"""
        with pytest.raises(FileNotFoundError):
            args.validate_config(config, schema)


class TestDB():
    """Testing config.db"""
    def test_failed_connection(self):
        """Failed connection raises BaseException"""
        with pytest.raises(BaseException):
            # invalid dbname
            db.db(dbname='invalid', host='localhost', port=5432, user='batch3dfier')
        with pytest.raises(BaseException):
            # invalid host
            db.db(dbname='batch3dfier_db', host='invalid', port=5432, user='batch3dfier')
        with pytest.raises(BaseException):
            # invalid port
            db.db(dbname='batch3dfier_db', host='localhost', port=1, user='batch3dfier')
        with pytest.raises(BaseException):
            # invalid user
            db.db(dbname='batch3dfier_db', host='localhost', port=5432, user='invalid')
        with pytest.raises(BaseException):
            # invalid password
            db.db(dbname='batch3dfier_db', host='localhost', port=5432, user='batch3dfier', password='invalid')
