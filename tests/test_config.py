import pytest
import os
import os.path

import yaml
import pykwalify.errors

from bag3d.config import args


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
        assert args.validate_config(config, schema) == True
    
    def test_config_invalid(self, invalid_config, schema):
        """Invalid schema"""
        assert args.validate_config(invalid_config, schema) == False