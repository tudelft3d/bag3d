import pytest
import bag3d.bag3dapp

from pprint import pprint

@pytest.fixture(scope="module",
                params=[["../example_data", "--export", "--create_db"]]
                        )
def cli_args(request):
    return request.param
    

def test_parser(cli_args):
    print(cli_args)
    args_in = bag3d.bag3dapp.parse_console_args(cli_args)
    pprint(args_in)
