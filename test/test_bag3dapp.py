import os.path

import pytest
import bag3d.bag3dapp

from pprint import pprint

@pytest.fixture(scope="module",
                params=[["./example_data", "--export", "--create_db"]]
                        )
def cli_args(request):
    return request.param

def test_parser(cli_args):
    d = os.path.abspath('.')
    f = os.path.join(d, 'example_data')
    t = {'cfg_dir': d,
         'cfg_file': f,
         'create_db': True,
         'export': True,
         'import_tile_idx': False,
         'run_3dfier': False,
         'threads': 3,
         'update_ahn': False,
         'update_bag': False}
    args_in = bag3d.bag3dapp.parse_console_args(cli_args)
    assert t == args_in

def test_parser_file_not_found():
    with pytest.raises(FileNotFoundError) as e_info:
        bag3d.bag3dapp.parse_console_args(["../example_data"])