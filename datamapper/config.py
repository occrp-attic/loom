import os
import yaml
import logging
import urlparse

from normality import slugify
from sqlalchemy import create_engine
from jsonschema import RefResolver

from datamapper.util import ConfigException
from datamapper.sinks import Sink

log = logging.getLogger(__name__)


class Config(dict):
    """ Parsing a configuration file. This specifies the database connection
    and the settings for each data sink. """

    def __init__(self, data, path=None, database=None):
        self.update(data)
        self.path = path
        self.database = database
        if 'schemas' not in self:
            self['schemas'] = {}

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
    def base_uri(self):
        if self.path is None:
            config_uri = 'file:///tmp'
        else:
            config_uri = 'file://' + os.path.abspath(self.path)
        return self.get('base_uri', config_uri)

    @property
    def schemas(self):
        return self.get('schemas', {})

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

    @property
    def sink(self):
        cls = Sink.by_name(self.get('sink'))
        if cls is None:
            raise ConfigException("No such sink type: %r" % self.get('sink'))
        return cls
