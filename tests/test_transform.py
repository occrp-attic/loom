from unittest import TestCase

from util import create_fixtures

class TransformTestCase(TestCase):

    def setUp(self):
        self.engine = create_fixtures()

    def tearDown(self):
        pass

    def test_load_spec(self):
        print self.engine
