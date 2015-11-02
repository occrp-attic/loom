import os
from unittest import TestCase

from util import create_fixtures, FIXTURE_PATH

from loom.db import Source, session
from loom.config import Config
from loom.spec import Spec
from loom.mapper import Mapper
from loom.util import load_config


class MapperTestCase(TestCase):

    def setUp(self):
        self.engine = create_fixtures()
        self.config = Config({})
        self.config._engine = self.engine
        self.config.setup()
        spec = load_config(os.path.join(FIXTURE_PATH, 'spec.yaml'))
        self.spec = Spec(self.config, spec)
        self.spec._engine = self.engine
        self.mapper = Mapper(self.config, self.spec)
        self.gen = self.mapper.generator

    def test_create_source(self):
        source = {'slug': 'foo', 'title': 'Foo source', 'url': 'http://foo'}
        Source.ensure(source)
        session.commit()
        cnt = session.query(Source).count()
        assert cnt == 1, cnt
        Source.ensure(source)
        session.commit()
        cnt = session.query(Source).count()
        assert cnt == 1, cnt

    def test_create_object(self):
        schema = self.spec.get('mappings').get('companies').get('schema')
        self.config.add_schema(schema)
        entity = {'id': 'foo', 'name': 'Foo entity'}
        assert len(self.config.types) == 0, len(self.config.types)
        self.config.entities.save(schema['id'], entity, 'foo_source')
        assert len(self.config.types) == 1, len(self.config.types)
