import pytest

from app import application
from app import application
from app.application import Application


class TestApplication:

    def test_always_passes(self):
        assert True


    def test_add_one(self):
        app = Application()
        assert app.add_one(3) == 4

    @pytest.mark.is_spark
    def test_can_agg(self):
        pass