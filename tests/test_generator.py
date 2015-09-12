import os
import yaml
from nose.tools import raises
from unittest import TestCase

from util import create_fixtures, FIXTURE_PATH

from datamapper.config import Config
from datamapper.generator import Generator, SpecException

class GeneratorTestCase(TestCase):

    def setUp(self):
        self.engine = create_fixtures()
        self.config = Config({})
        self.config._engine = self.engine
        with open(os.path.join(FIXTURE_PATH, 'spec.yaml'), 'r') as fh:
            self.spec = yaml.load(fh)
        self.gen = Generator(self.config, self.spec)

    def tearDown(self):
        pass

    def test_load_spec(self):
        assert len(self.gen.tables) == 2
        col = self.gen.get_column('financials.price')
        assert col is not None, col

    @raises(SpecException)
    def test_invalid_table(self):
        col = self.gen.get_column('financials_xxx.price')

    @raises(SpecException)
    def test_no_table(self):
        col = self.gen.get_column('price')

    @raises(SpecException)
    def test_invalid_column(self):
        col = self.gen.get_column('financials.value')

    @raises(SpecException)
    def test_invalid_output(self):
        for x in self.gen.generate('knuffels'):
            pass

    def test_generate_partial_output(self):
        comps = [r[1] for r in self.gen.generate('companies')]
        assert 'companies.symbol' in comps[0], comps[0]
        assert 'companies.sector' not in comps[0], comps[0]
        assert len(comps) == 496, len(comps)

    def test_generate_full_output(self):
        comps = [r[1] for r in self.gen.generate('companies', full_tables=True)]
        assert 'companies.sector' in comps[0], comps[0]
        assert len(comps) == 496, len(comps)

    def test_generate_mapping(self):
        comps = [r[0] for r in self.gen.generate('companies')]
        assert 'id' in comps[0], comps[0]
        assert 'sector' not in comps[0], comps[0]
        assert isinstance(comps[0]['financials']['price'], float), comps[0]
        assert len(comps) == 496, len(comps)
