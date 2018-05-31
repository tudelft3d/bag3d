import pytest
import os
import os.path

import yaml

from bag3d.config import args

from pykwalify.core import Core
import pykwalify.errors

class TestArgs():
    """Testing config.args"""
    def test_config_validation(self, config, schema):
        c = Core(source_file=config, schema_files=[schema])
        try:
            c.validate(raise_exception=True)
        except pykwalify.errors.SchemaConflict as e:
            print(e)
            raise