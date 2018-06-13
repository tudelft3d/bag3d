# -*- coding: utf-8 -*-

"""Testing config.border"""

import os.path

import pytest
import logging

from bag3d.config import db
from bag3d.config import border

@pytest.fixture('module')
def conn():
    yield db.db(
        dbname='batch3dfier_db',
        host='localhost',
        port=5432,
        user='batch3dfier')

@pytest.fixture('module')
def cfg():
    yield {'bag3d_table': 'bag3d',
 'config': {'in': '/home/balazs/Development/bag3d/bag3d_config.yml',
            'out_border_ahn2': '/home/balazs/Development/bag3d/bag3d_cfg_border_ahn2.yml',
            'out_border_ahn3': '/home/balazs/Development/bag3d/bag3d_cfg_border_ahn3.yml',
            'out_rest': '/home/balazs/Development/bag3d/bag3d_cfg_rest.yml'},
 'database': {'dbname': 'batch3dfier_db',
              'host': 'localhost',
              'port': 5432,
              'pw': None,
              'user': 'batch3dfier'},
 'elevation': {'border_table': 'border_tiles',
               'fields': {'geometry': 'geom',
                          'primary_key': 'id',
                          'unit_name': 'bladnr',
                          'version': 'ahn_version'},
               'file': 'example_data/ahn_index.json',
               'schema': 'tile_index',
               'table': 'ahn_index'},
 'extent_file': '/home/bdukai/Development/batch3dfier/example_data/extent_small.geojson',
 'footprints': {'fields': {'geometry': 'geom',
                           'primary_key': 'gid',
                           'uniqueid': 'identificatie'},
                'schema': 'bagactueel',
                'table': 'pand'},
 'input_elevation': {'dataset_dir': ['/example_data/ahn3',
                                     '/example_data/ahn2/merged'],
                     'dataset_name': ['C_{tile}.LAZ', '{tile}.laz']},
 'out_schema': 'bagactueel',
 'out_table': 'heights',
 'output_dir': '/tmp/3DBAG',
 'path_3dfier': '/home/bdukai/Development/3dfier/build/3dfier',
 'pc_dir': ['/example_data/ahn3', '/example_data/ahn2/merged'],
 'polygons': {'fields': {'geometry': 'geom',
                         'primary_key': 'id',
                         'unit_name': 'bladnr'},
              'file': 'example_data/bag_index_test.json',
              'schema': 'tile_index',
              'table': 'bag_index_test'},
 'prefix_tile_footprint': 't_',
 'tile_schema': 'bag_tiles',
 'tiles': None,
 'user_schema': 'bag_tiles'}


class TestBorder():
    def test_create_border_table(self, caplog, conn, cfg):
        tbl_schema = cfg['elevation']['schema']
        tbl_name = cfg['elevation']['table']
        tbl_tile = cfg['elevation']['fields']['unit_name']
        tbl_version = cfg['elevation']['fields']['version']
        tbl_geom = cfg['elevation']['fields']['geometry']
        border_table = cfg['elevation']['border_table']
        with caplog.at_level(logging.DEBUG):
            border.create_border_table(conn, idx_schema=tbl_schema, 
                               idx_table=tbl_name, 
                               idx_table_version=tbl_version, 
                               idx_table_geom=tbl_geom, 
                               border_table=border_table,
                               doexec=False)
            
            