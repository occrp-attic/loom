import os
from nose.tools import raises
from unittest import TestCase

from util import create_fixtures, FIXTURE_PATH

from loom.config import Config
from loom.spec import Spec
from loom.mapper import Mapper
from loom.util import SpecException, ConfigException, load_config


class MapperTestCase(TestCase):

    def setUp(self):
        self.engine = create_fixtures()
        self.config = Config({

        })
        self.config._engine = self.engine
        spec = load_config(os.path.join(FIXTURE_PATH, 'spec.yaml'))
        self.spec = Spec(self.config, spec)
        self.spec._engine = self.engine
        self.mapper = Mapper(self.config, self.spec)
        self.gen = self.mapper.generator

    def tearDown(self):
        pass

    @raises(SpecException)
    def test_invalid_table(self):
        self.gen.get_column('financials_xxx.price')

    @raises(SpecException)
    def test_no_table(self):
        self.gen.get_column('price')

    @raises(SpecException)
    def test_invalid_column(self):
        self.gen.get_column('fin.value')

    @raises(SpecException)
    def test_unaliased_table(self):
        self.gen.get_column('financials.value')

    @raises(ConfigException)
    def test_invalid_output(self):
        for x in self.gen.generate('knuffels'):
            pass

    def test_generate_records(self):
        comps = [e for e in self.gen.generate('companies')]
        assert 'companies.symbol' in comps[0], comps[0]
        assert 'fin' not in comps[0], comps[0]
        assert len(comps) == 496, len(comps)

    def test_mapping_records(self):
        for stmt in self.mapper.records('companies'):
            assert 'companies.symbol' not in stmt
            (s, p, o, t) = stmt
            assert s is not None

    def test_map_objects(self):
        self.mapper.map()
        subject = 'sp500:MMM:MMM'
        data = self.config.entities.get(subject)
        assert '$schema' in data, data
        assert data['name'] == '3M Co', data
        assert len(self.config.types) == 992, len(self.config.types)
