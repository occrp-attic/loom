import os
import yaml
import logging
import urlparse

from normality import slugify
from sqlalchemy import create_engine
from jsonschema import RefResolver
from jsongraph import Graph

from datamapper.db import StatementTable
from datamapper.util import ConfigException, EnvMapping

log = logging.getLogger(__name__)


class Config(EnvMapping):
    """ Parsing a configuration file. This specifies the database connection
    and the settings for each data sink. """

    def __init__(self, data, path=None, database=None):
        if 'schemas' not in data:
            data['schemas'] = {}
        self.data = data
        self.path = path
        self.database = database

    @classmethod
    def from_path(cls, path, **kwargs):
        with open(path, 'r') as fh:
            return cls(yaml.load(fh), path=path, **kwargs)

    @property
    def engine(self):
        if not hasattr(self, '_engine'):
            database = self.database or self.get('database')
            if database is None:
                raise ConfigException("No database URI configued!")
            log.debug("Database: %r", database)
            self._engine = create_engine(database)
        return self._engine

    @property
    def statement(self):
        if not hasattr(self, '_statement'):
            self._statement = StatementTable(self.engine)
            if not self._statement.exists:
                self._statement.create()
        return self._statement

    @property
    def base_uri(self):
        if self.path is None:
            config_uri = 'file:///tmp'
        else:
            config_uri = 'file://' + os.path.abspath(self.path)
        return self.get('base_uri') or config_uri

    @property
    def schemas(self):
        return self.get('schemas') or {}

    @property
    def graph(self):
        if not hasattr(self, '_graph'):
            self._graph = Graph(base_uri=self.base_uri,
                                resolver=self.resolver,
                                config=self)
            for alias, uri in self.schemas.items():
                self._graph.register(alias, uri)
            log.debug("Graph: %r", self._graph)
        return self._graph

    @property
    def resolver(self):
        if not hasattr(self, '_resolver'):
            self._resolver = RefResolver(self.base_uri, self.base_uri)
            for alias, uri in self.schemas.items():
                self._resolver.resolve(uri)
        return self._resolver

    def add_schema(self, schema):
        if 'id' in schema and schema['id'] not in self.resolver.store:
            self.resolver.store[schema['id']] = schema

    def get_alias(self, schema):
        """ Slightly hacky way of getting a slug-like name for a schema. This
        is used to determine document types in the Elastic index. """
        for alias, uri in self.schemas.items():
            if uri == schema:
                return alias
        p = urlparse.urlparse(schema)
        name, _ = os.path.splitext(os.path.basename(p.path))
        name = slugify(name, sep='_')
        if not len(name) or name in self.schemas:
            raise ConfigException("Cannot determine alias for: %r" % schema)
        self['schemas'][name] = schema
        return name
