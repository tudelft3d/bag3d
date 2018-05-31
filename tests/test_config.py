import pytest
import os
import os.path

import yaml

from bag3d.config import args


class TestArgs():
    """Testing config.args"""
    def test_config_validation(self, config, schema):
        """Schema validation"""
        assert args.validate_config(config, schema) == True