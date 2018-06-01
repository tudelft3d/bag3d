from datetime import date

import pytest

from bag3d.update import bag

class TestBAG():
    """Testing the BAG module"""
    def test_get_latest_bag(self):
        d = bag.get_latest_BAG()
        assert isinstance(d, date)