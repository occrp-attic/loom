import logging

from sqlalchemy import create_engine

from loom.db import Source
from loom.util import ConfigException, EnvMapping

log = logging.getLogger(__name__)


class Spec(EnvMapping):
    """ Parsing a spec file. This specifies the database connection
    and the settings for each data mapping. """

    def __init__(self, config, data, path=None):
        self.config = config
        self.path = path
        super(Spec, self).__init__(data)

    @property
    def source(self):
        if not hasattr(self, '_source_id'):
            source = self.get('source')
            log.info("Registering source: %r", source['slug'])
            self._source_id = Source.ensure(source).id
        return self._source_id

    @property
    def engine(self):
        if not hasattr(self, '_engine'):
            uri = self.get('database')
            if uri is None:
                raise ConfigException("No source database URI configured!")
            log.debug("Source database: %r", uri)
            self._engine = create_engine(uri)
        return self._engine

    @property
    def mappings(self):
        return self.get('mappings', {}).keys()

    def get_mapping(self, name):
        mapping = self.get('mappings', {}).get(name, raw=True)
        if mapping is None:
            raise ConfigException("No such mapping: %r", name)
        self.config.add_schema(mapping['schema'])
        return mapping
