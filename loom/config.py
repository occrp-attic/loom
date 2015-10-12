import os
import logging
import urlparse

from normality import slugify
from sqlalchemy import create_engine
from jsonschema import RefResolver
from elasticsearch import Elasticsearch

from loom.db import get_entities_manager, get_properties_manager
from loom.util import ConfigException, EnvMapping

log = logging.getLogger(__name__)


class Config(EnvMapping):
    """ Parsing a configuration file. This specifies the database connection
    and the settings for each data sink. """

    def __init__(self, data, path=None):
        self.path = path or os.getcwd()
        data['schemas'] = data.get('schemas', {})
        super(Config, self).__init__(data)

    @property
    def source(self):
        return unicode(self.get('source', {}).get('slug'))

    @property
    def ods(self):
        if not hasattr(self, '_ods'):
            uri = self.get('ods_database')
            if uri is None:
                raise ConfigException("No source database URI configured!")
            log.info("Source database: %r", uri)
            self._ods = create_engine(uri)
        return self._ods

    @property
    def engine(self):
        if not hasattr(self, '_engine'):
            uri = self.get('loom_database')
            if uri is None:
                raise ConfigException("No statement database URI configured!")
            log.info("Loom database: %r", uri)
            self._engine = create_engine(uri)
        return self._engine

    @property
    def entities(self):
        if not hasattr(self, '_entities'):
            self._entities = get_entities_manager(self)
            self._entities.create()
        return self._entities

    @property
    def properties(self):
        if not hasattr(self, '_properties'):
            self._properties = get_properties_manager(self)
            self._properties.create()
        return self._properties

    @property
    def base_uri(self):
        uri = 'file://' + os.path.abspath(self.path)
        return self.get('base_uri', uri)

    @property
    def mappings(self):
        return self.get('mappings', {}).keys()

    def get_mapping(self, name):
        mapping = self.get('mappings', {}).get(name, raw=True)
        if mapping is None:
            raise ConfigException("No such mapping: %r", name)
        self.add_schema(mapping['schema'])
        return mapping

    @property
    def elastic_client(self):
        if not hasattr(self, '_elastic_client'):
            host = self.get('elastic_host')
            if host is None:
                raise ConfigException("No 'elastic_host' is configured.")
            self._elastic_client = Elasticsearch([host])
        return self._elastic_client

    @property
    def elastic_index(self):
        if not hasattr(self, '_elastic_index'):
            self._elastic_index = self.get('elastic_index')
            if self._elastic_index is None:
                raise ConfigException("No 'elastic_index' is configured.")
        return self._elastic_index

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
