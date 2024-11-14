from unittest import TestCase

from app.application import Application


class TestApplication(TestCase):
    def test_increment_by_two(self):
        app = Application()
        assert app.increment_by_two(4) == 6
